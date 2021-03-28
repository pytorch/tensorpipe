/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_ipc/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <cuda.h>
#include <cuda_runtime.h>
#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/types/variant.h>

#include <tensorpipe/channel/cuda_ipc/context_impl.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

namespace {

struct Descriptor {
  std::string allocationId;
  std::string handle;
  size_t offset;
  std::string startEvHandle;
  NOP_STRUCTURE(Descriptor, allocationId, handle, offset, startEvHandle);
};

struct Reply {
  std::string stopEvHandle;
  NOP_STRUCTURE(Reply, stopEvHandle);
};

struct Ack {
  NOP_STRUCTURE(Ack);
};

} // namespace

SendOperation::SendOperation(
    TSendCallback callback,
    int deviceIdx,
    const void* ptr,
    cudaStream_t stream)
    : ptr(ptr),
      deviceIdx(deviceIdx),
      stream(stream),
      callback(std::move(callback)) {}

RecvOperation::RecvOperation(
    int deviceIdx,
    void* ptr,
    cudaStream_t stream,
    size_t length)
    : ptr(ptr), length(length), deviceIdx(deviceIdx), stream(stream) {}

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> replyConnection,
    std::shared_ptr<transport::Connection> ackConnection)
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      replyConnection_(std::move(replyConnection)),
      ackConnection_(std::move(ackConnection)) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    TSendCallback callback) {
  int deviceIdx = cudaDeviceForPointer(
      context_->getCudaLib(), buffer.unwrap<CudaBuffer>().ptr);

  SendOpIter opIter = sendOps_.emplaceBack(
      sequenceNumber,
      std::move(callback),
      deviceIdx,
      buffer.unwrap<CudaBuffer>().ptr,
      buffer.unwrap<CudaBuffer>().stream);

  sendOps_.advanceOperation(opIter);
}

void ChannelImpl::advanceSendOperation(
    SendOpIter opIter,
    SendOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  SendOperation& op = *opIter;

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_,
      /*actions=*/
      {&ChannelImpl::callDescriptorCallback, &ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure later operations are not holding
  // events while earlier ones are still blocked waiting for them, because the
  // events will only be returned after the control messages have been written
  // and sent, and this won't happen for later operations until earlier ones
  // have reached that stage too, and if those are blocked waiting for events
  // then we may deadlock.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::REQUESTING_EVENT,
      /*cond=*/!error_ && prevOpState >= SendOperation::REQUESTING_EVENT,
      /*actions=*/{&ChannelImpl::requestEvent});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::REQUESTING_EVENT,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_ && op.doneRequestingEvent,
      /*actions=*/
      {&ChannelImpl::callDescriptorCallback,
       &ChannelImpl::returnEvent,
       &ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on reply control connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::REQUESTING_EVENT,
      /*to=*/SendOperation::READING_REPLY,
      /*cond=*/!error_ && op.doneRequestingEvent &&
          prevOpState >= SendOperation::READING_REPLY,
      /*actions=*/
      {&ChannelImpl::recordStartEvent,
       &ChannelImpl::callDescriptorCallback,
       &ChannelImpl::readReply});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::READING_REPLY,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_ && op.doneReadingReply,
      /*actions=*/{&ChannelImpl::returnEvent, &ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on ack control connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::READING_REPLY,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/!error_ && op.doneReadingReply &&
          prevOpState >= SendOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::returnEvent,
       &ChannelImpl::waitOnStopEvent,
       &ChannelImpl::callSendCallback,
       &ChannelImpl::writeAck});
}

void ChannelImpl::requestEvent(SendOpIter opIter) {
  SendOperation& op = *opIter;

  context_->requestSendEvent(
      op.deviceIdx,
      callbackWrapper_(
          [opIter](ChannelImpl& impl, CudaEventPool::BorrowedEvent event) {
            opIter->doneRequestingEvent = true;
            if (!impl.error_) {
              opIter->startEv = std::move(event);
            }
            impl.sendOps_.advanceOperation(opIter);
          }));
}

void ChannelImpl::recordStartEvent(SendOpIter opIter) {
  SendOperation& op = *opIter;

  op.startEv->record(op.stream);
}

void ChannelImpl::callDescriptorCallback(SendOpIter opIter) {
  SendOperation& op = *opIter;

  if (error_) {
    return;
  }

  const CudaLib& cudaLib = context_->getCudaLib();

  cudaIpcMemHandle_t handle;
  size_t offset;
  unsigned long long bufferId;
  {
    CudaDeviceGuard guard(op.deviceIdx);
    TP_CUDA_CHECK(cudaIpcGetMemHandle(&handle, const_cast<void*>(op.ptr)));

    CUdeviceptr basePtr;
    TP_CUDA_DRIVER_CHECK(
        cudaLib,
        cudaLib.memGetAddressRange(
            &basePtr, nullptr, reinterpret_cast<CUdeviceptr>(op.ptr)));
    offset = reinterpret_cast<const uint8_t*>(op.ptr) -
        reinterpret_cast<uint8_t*>(basePtr);

    TP_CUDA_DRIVER_CHECK(
        cudaLib,
        cudaLib.pointerGetAttribute(
            &bufferId, CU_POINTER_ATTRIBUTE_BUFFER_ID, basePtr));
  }

  NopHolder<Descriptor> nopHolder;
  nopHolder.getObject() = Descriptor{
      // FIXME The process identifier will be the same each time, hence we could
      // just send it once during the setup of the channel and omit it later.
      context_->getProcessIdentifier() + "_" + std::to_string(bufferId),
      std::string(reinterpret_cast<const char*>(&handle), sizeof(handle)),
      offset,
      op.startEv->serializedHandle()};
}

void ChannelImpl::readReply(SendOpIter opIter) {
  SendOperation& op = *opIter;

  auto nopReplyHolder = std::make_shared<NopHolder<Reply>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (reply #"
             << op.sequenceNumber << ")";
  replyConnection_->read(
      *nopReplyHolder,
      callbackWrapper_([opIter, nopReplyHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (reply #"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingReply = true;
        if (!impl.error_) {
          opIter->stopEvHandle =
              std::move(nopReplyHolder->getObject().stopEvHandle);
        }
        impl.sendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::returnEvent(SendOpIter opIter) {
  SendOperation& op = *opIter;

  op.startEv = nullptr;
}

void ChannelImpl::waitOnStopEvent(SendOpIter opIter) {
  SendOperation& op = *opIter;

  const cudaIpcEventHandle_t* stopEvHandle =
      reinterpret_cast<const cudaIpcEventHandle_t*>(op.stopEvHandle.c_str());
  CudaEvent stopEv(op.deviceIdx, *stopEvHandle);
  stopEv.wait(op.stream, op.deviceIdx);
}

void ChannelImpl::callSendCallback(SendOpIter opIter) {
  SendOperation& op = *opIter;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
}

void ChannelImpl::writeAck(SendOpIter opIter) {
  SendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is writing ACK notification (#"
             << op.sequenceNumber << ")";
  auto nopAckHolder = std::make_shared<NopHolder<Ack>>();
  ackConnection_->write(
      *nopAckHolder,
      callbackWrapper_(
          [nopAckHolder, sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done writing ACK notification (#" << sequenceNumber
                       << ")";
          }));
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    TRecvCallback callback) {
  int deviceIdx = cudaDeviceForPointer(
      context_->getCudaLib(), buffer.unwrap<CudaBuffer>().ptr);
  RecvOpIter opIter = recvOps_.emplaceBack(
      sequenceNumber,
      deviceIdx,
      buffer.unwrap<CudaBuffer>().ptr,
      buffer.unwrap<CudaBuffer>().stream,
      buffer.unwrap<CudaBuffer>().length);

  opIter->callback = std::move(callback);

  NopHolder<Descriptor> nopHolder;
  // FIXME
  // loadDescriptor(nopHolder, descriptor);
  Descriptor& nopDescriptor = nopHolder.getObject();
  opIter->allocationId = std::move(nopDescriptor.allocationId);
  opIter->startEvHandle = std::move(nopDescriptor.startEvHandle);
  opIter->bufferHandle = std::move(nopDescriptor.handle);
  opIter->offset = nopDescriptor.offset;

  recvOps_.advanceOperation(opIter);
}

void ChannelImpl::advanceRecvOperation(
    RecvOpIter opIter,
    RecvOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  RecvOperation& op = *opIter;

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure later operations are not holding
  // events while earlier ones are still blocked waiting for them, because the
  // events will only be returned after the control messages have been written
  // and sent, and this won't happen for later operations until earlier ones
  // have reached that stage too, and if those are blocked waiting for events
  // then we may deadlock.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::REQUESTING_EVENT,
      /*cond=*/!error_ && prevOpState >= RecvOperation::REQUESTING_EVENT,
      /*actions=*/
      {&ChannelImpl::requestEvent});

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::REQUESTING_EVENT,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_ && op.doneRequestingEvent,
      /*actions=*/{&ChannelImpl::callRecvCallback, &ChannelImpl::returnEvent});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on reply control connection and read calls on ack control
  // connection.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::REQUESTING_EVENT,
      /*to=*/RecvOperation::READING_ACK,
      /*cond=*/!error_ && op.doneRequestingEvent &&
          prevOpState >= RecvOperation::READING_ACK,
      /*actions=*/
      {&ChannelImpl::waitOnStartEventAndCopyAndRecordStopEvent,
       &ChannelImpl::callRecvCallback,
       &ChannelImpl::writeReplyAndReadAck});

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::READING_ACK,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/op.doneReadingAck,
      /*actions=*/{&ChannelImpl::returnEvent});
}

void ChannelImpl::requestEvent(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  context_->requestRecvEvent(
      op.deviceIdx,
      callbackWrapper_(
          [opIter](ChannelImpl& impl, CudaEventPool::BorrowedEvent event) {
            opIter->doneRequestingEvent = true;
            if (!impl.error_) {
              opIter->stopEv = std::move(event);
            }
            impl.recvOps_.advanceOperation(opIter);
          }));
}

void ChannelImpl::waitOnStartEventAndCopyAndRecordStopEvent(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  const cudaIpcEventHandle_t* startEvHandle =
      reinterpret_cast<const cudaIpcEventHandle_t*>(op.startEvHandle.c_str());
  const cudaIpcMemHandle_t* remoteHandle =
      reinterpret_cast<const cudaIpcMemHandle_t*>(op.bufferHandle.c_str());

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#"
             << op.sequenceNumber << ")";

  CudaEvent startEv(op.deviceIdx, *startEvHandle);
  startEv.wait(op.stream, op.deviceIdx);

  void* remoteBasePtr =
      context_->openIpcHandle(op.allocationId, *remoteHandle, op.deviceIdx);
  {
    CudaDeviceGuard guard(op.deviceIdx);
    TP_CUDA_CHECK(cudaMemcpyAsync(
        op.ptr,
        static_cast<uint8_t*>(remoteBasePtr) + op.offset,
        op.length,
        cudaMemcpyDeviceToDevice,
        op.stream));
  }

  op.stopEv->record(op.stream);

  TP_VLOG(6) << "Channel " << id_ << " done copying payload (#"
             << op.sequenceNumber << ")";
}

void ChannelImpl::callRecvCallback(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
}

void ChannelImpl::writeReplyAndReadAck(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is writing reply notification (#"
             << op.sequenceNumber << ")";
  auto nopReplyHolder = std::make_shared<NopHolder<Reply>>();
  Reply& nopReply = nopReplyHolder->getObject();
  nopReply.stopEvHandle = op.stopEv->serializedHandle();
  replyConnection_->write(
      *nopReplyHolder,
      callbackWrapper_([nopReplyHolder,
                        sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing reply notification (#" << sequenceNumber
                   << ")";
      }));

  TP_VLOG(6) << "Channel " << id_ << " is reading ACK notification (#"
             << op.sequenceNumber << ")";
  auto nopAckHolder = std::make_shared<NopHolder<Ack>>();
  ackConnection_->read(
      *nopAckHolder,
      callbackWrapper_([opIter, nopAckHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading ACK notification (#"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingAck = true;
        impl.recvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::returnEvent(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  op.stopEv = nullptr;
}

void ChannelImpl::handleErrorImpl() {
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();

  replyConnection_->close();
  ackConnection_->close();

  context_->unenroll(*this);
}

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
