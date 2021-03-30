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

#include <tensorpipe/channel/cuda_ipc/constants.h>
#include <tensorpipe/channel/cuda_ipc/context_impl.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

namespace {

size_t ceilOfRatio(size_t n, size_t d) {
  return (n + d - 1) / d;
}

struct Descriptor {
  std::string allocationId;
  std::string handle;
  size_t offset;
  std::string eventHandle;
  NOP_STRUCTURE(Descriptor, allocationId, handle, offset, eventHandle);
};

} // namespace

ChunkSendOperation::ChunkSendOperation(
    uint64_t bufferSequenceNumber,
    size_t chunkId,
    size_t numChunks,
    TSendCallback callback,
    int deviceIdx,
    const void* ptr,
    size_t length,
    cudaStream_t stream)
    : bufferSequenceNumber(bufferSequenceNumber),
      chunkId(chunkId),
      numChunks(numChunks),
      ptr(ptr),
      length(length),
      deviceIdx(deviceIdx),
      stream(stream),
      callback(std::move(callback)) {}

ChunkRecvOperation::ChunkRecvOperation(
    uint64_t bufferSequenceNumber,
    size_t chunkId,
    size_t numChunks,
    TRecvCallback callback,
    int deviceIdx,
    void* ptr,
    size_t length,
    cudaStream_t stream)
    : bufferSequenceNumber(bufferSequenceNumber),
      chunkId(chunkId),
      numChunks(numChunks),
      ptr(ptr),
      length(length),
      deviceIdx(deviceIdx),
      stream(stream),
      callback(std::move(callback)) {}

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> descriptorConnection,
    std::shared_ptr<transport::Connection> replyConnection)
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      descriptorConnection_(std::move(descriptorConnection)),
      replyConnection_(std::move(replyConnection)) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  int deviceIdx = cudaDeviceForPointer(
      context_->getCudaLib(), buffer.unwrap<CudaBuffer>().ptr);
  const size_t bufferLength = buffer.unwrap<CudaBuffer>().length;
  const size_t numChunks = ceilOfRatio(bufferLength, kSlotSize);

  for (size_t chunkIdx = 0; chunkIdx < numChunks; chunkIdx += 1) {
    size_t offset = chunkIdx * kSlotSize;
    ChunkSendOpIter opIter = chunkSendOps_.emplaceBack(
        nextChunkBeingSent_++,
        sequenceNumber,
        chunkIdx,
        numChunks,
        chunkIdx == numChunks - 1 ? std::move(callback) : nullptr,
        deviceIdx,
        reinterpret_cast<uint8_t*>(buffer.unwrap<CudaBuffer>().ptr) + offset,
        std::min(bufferLength - offset, kSlotSize),
        buffer.unwrap<CudaBuffer>().stream);

    chunkSendOps_.advanceOperation(opIter);
  }

  descriptorCallback(error_, "");
}

void ChannelImpl::advanceChunkSendOperation(
    ChunkSendOpIter opIter,
    ChunkSendOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  ChunkSendOperation& op = *opIter;

  // Needs to go after previous op invoked its callback because the last chunk
  // in a series (that corresponds to one operation) must invoke its callback
  // only when all chunks in the series are done.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::UNINITIALIZED,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_ && prevOpState >= ChunkSendOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure later operations are not holding
  // events while earlier ones are still blocked waiting for them, because the
  // events will only be returned after the control messages have been written
  // and sent, and this won't happen for later operations until earlier ones
  // have reached that stage too, and if those are blocked waiting for events
  // then we may deadlock.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::UNINITIALIZED,
      /*to=*/ChunkSendOperation::REQUESTING_EVENT,
      /*cond=*/!error_ && prevOpState >= ChunkSendOperation::REQUESTING_EVENT,
      /*actions=*/{&ChannelImpl::requestEvent});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::REQUESTING_EVENT,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_ && op.doneRequestingEvent &&
          prevOpState >= ChunkSendOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::returnEvent, &ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the descriptor control connection and read calls on the
  // reply control connection.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::REQUESTING_EVENT,
      /*to=*/ChunkSendOperation::READING_REPLY,
      /*cond=*/!error_ && op.doneRequestingEvent &&
          prevOpState >= ChunkSendOperation::READING_REPLY,
      /*actions=*/
      {&ChannelImpl::recordStartEvent,
       &ChannelImpl::writeDescriptor,
       &ChannelImpl::readReply});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::READING_REPLY,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_ && op.doneReadingReply &&
          prevOpState >= ChunkSendOperation::FINISHED,
      /*actions=*/{&ChannelImpl::returnEvent, &ChannelImpl::callSendCallback});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::READING_REPLY,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/!error_ && op.doneReadingReply &&
          prevOpState >= ChunkSendOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::waitOnStopEvent,
       &ChannelImpl::returnEvent,
       &ChannelImpl::callSendCallback});
}

void ChannelImpl::requestEvent(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  context_->requestSendEvent(
      op.deviceIdx,
      callbackWrapper_(
          [opIter](ChannelImpl& impl, CudaEventPool::BorrowedEvent event) {
            opIter->doneRequestingEvent = true;
            if (!impl.error_) {
              opIter->event = std::move(event);
            }
            impl.chunkSendOps_.advanceOperation(opIter);
          }));
}

void ChannelImpl::recordStartEvent(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  op.event->record(op.stream);
}

void ChannelImpl::writeDescriptor(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

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

  auto nopDescriptorHolder = std::make_shared<NopHolder<Descriptor>>();
  Descriptor& nopDescriptor = nopDescriptorHolder->getObject();
  // FIXME The process identifier will be the same each time, hence we could
  // just send it once during the setup of the channel and omit it later.
  nopDescriptor.allocationId =
      context_->getProcessIdentifier() + "_" + std::to_string(bufferId);
  nopDescriptor.handle =
      std::string(reinterpret_cast<const char*>(&handle), sizeof(handle));
  nopDescriptor.offset = offset;
  nopDescriptor.eventHandle = op.event->serializedHandle();
  TP_VLOG(6) << "Channel " << id_ << " is writing nop object (descriptor #"
             << op.sequenceNumber << ")";
  descriptorConnection_->write(
      *nopDescriptorHolder,
      callbackWrapper_([nopDescriptorHolder,
                        sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (descriptor #" << sequenceNumber
                   << ")";
      }));
}

void ChannelImpl::readReply(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (reply #"
             << op.sequenceNumber << ")";
  replyConnection_->read(
      nullptr,
      0,
      callbackWrapper_([opIter](
                           ChannelImpl& impl,
                           const void* /* unused */,
                           size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (reply #"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingReply = true;
        impl.chunkSendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::waitOnStopEvent(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  op.event->wait(op.stream, op.deviceIdx);
}

void ChannelImpl::returnEvent(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  op.event = nullptr;
}

void ChannelImpl::callSendCallback(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  if (op.callback) {
    op.callback(error_);
    // Reset callback to release the resources it was holding.
    op.callback = nullptr;
  }
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    Buffer buffer,
    TRecvCallback callback) {
  TP_DCHECK_EQ(descriptor, "");

  int deviceIdx = cudaDeviceForPointer(
      context_->getCudaLib(), buffer.unwrap<CudaBuffer>().ptr);
  const size_t bufferLength = buffer.unwrap<CudaBuffer>().length;
  const size_t numChunks = ceilOfRatio(bufferLength, kSlotSize);

  for (size_t chunkIdx = 0; chunkIdx < numChunks; chunkIdx += 1) {
    size_t offset = chunkIdx * kSlotSize;
    ChunkRecvOpIter opIter = chunkRecvOps_.emplaceBack(
        nextChunkBeingReceived_++,
        sequenceNumber,
        chunkIdx,
        numChunks,
        chunkIdx == numChunks - 1 ? std::move(callback) : nullptr,
        deviceIdx,
        reinterpret_cast<uint8_t*>(buffer.unwrap<CudaBuffer>().ptr) + offset,
        std::min(bufferLength - offset, kSlotSize),
        buffer.unwrap<CudaBuffer>().stream);

    chunkRecvOps_.advanceOperation(opIter);
  }
}

void ChannelImpl::advanceChunkRecvOperation(
    ChunkRecvOpIter opIter,
    ChunkRecvOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  ChunkRecvOperation& op = *opIter;

  // Needs to go after previous op invoked its callback because the last chunk
  // in a series (that corresponds to one operation) must invoke its callback
  // only when all chunks in the series are done.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::UNINITIALIZED,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/error_ && prevOpState >= ChunkRecvOperation::FINISHED,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on descriptor control connection.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::UNINITIALIZED,
      /*to=*/ChunkRecvOperation::READING_DESCRIPTOR,
      /*cond=*/!error_ && prevOpState >= ChunkRecvOperation::READING_DESCRIPTOR,
      /*actions=*/{&ChannelImpl::readDescriptor});

  // See above for why this needs to go after previous op.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::READING_DESCRIPTOR,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/error_ && op.doneReadingDescriptor &&
          prevOpState >= ChunkRecvOperation::FINISHED,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on reply control connection.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::READING_DESCRIPTOR,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/!error_ && op.doneReadingDescriptor &&
          prevOpState >= ChunkRecvOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::waitOnStartEventAndCopyAndRecordStopEvent,
       &ChannelImpl::callRecvCallback,
       &ChannelImpl::writeReply});
}

void ChannelImpl::readDescriptor(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (descriptor #"
             << op.sequenceNumber << ")";
  auto nopDescriptorHolder = std::make_shared<NopHolder<Descriptor>>();
  descriptorConnection_->read(
      *nopDescriptorHolder,
      callbackWrapper_([opIter, nopDescriptorHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (descriptor #"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingDescriptor = true;
        if (!impl.error_) {
          Descriptor& nopDescriptor = nopDescriptorHolder->getObject();
          opIter->allocationId = std::move(nopDescriptor.allocationId);
          opIter->bufferHandle = std::move(nopDescriptor.handle);
          opIter->offset = nopDescriptor.offset;
          opIter->eventHandle = std::move(nopDescriptor.eventHandle);
        }
        impl.chunkRecvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::waitOnStartEventAndCopyAndRecordStopEvent(
    ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  const cudaIpcEventHandle_t* startEvHandle =
      reinterpret_cast<const cudaIpcEventHandle_t*>(op.eventHandle.c_str());
  const cudaIpcMemHandle_t* remoteHandle =
      reinterpret_cast<const cudaIpcMemHandle_t*>(op.bufferHandle.c_str());

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#"
             << op.sequenceNumber << ")";

  CudaEvent event(op.deviceIdx, *startEvHandle);
  event.wait(op.stream, op.deviceIdx);

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

  event.record(op.stream);

  TP_VLOG(6) << "Channel " << id_ << " done copying payload (#"
             << op.sequenceNumber << ")";
}

void ChannelImpl::callRecvCallback(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  if (op.callback) {
    op.callback(error_);
    // Reset callback to release the resources it was holding.
    op.callback = nullptr;
  }
}

void ChannelImpl::writeReply(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is writing reply notification (#"
             << op.sequenceNumber << ")";
  replyConnection_->write(
      nullptr,
      0,
      callbackWrapper_([sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing reply notification (#" << sequenceNumber
                   << ")";
      }));
}

void ChannelImpl::handleErrorImpl() {
  chunkSendOps_.advanceAllOperations();
  chunkRecvOps_.advanceAllOperations();

  descriptorConnection_->close();
  replyConnection_->close();

  context_->unenroll(*this);
}

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
