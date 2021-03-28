/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_xth/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <cuda_runtime.h>
#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cuda_xth/context_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_xth {

namespace {

struct Descriptor {
  uintptr_t startEvent;
  uintptr_t srcPtr;
  int srcDeviceIdx;
  uintptr_t srcStream;
  NOP_STRUCTURE(Descriptor, startEvent, srcPtr, srcDeviceIdx, srcStream);
};

} // namespace

SendOperation::SendOperation(
    int deviceIdx,
    void* ptr,
    cudaStream_t stream,
    TSendCallback callback)
    : deviceIdx(deviceIdx),
      ptr(ptr),
      stream(stream),
      callback(std::move(callback)),
      startEv(deviceIdx) {
  startEv.record(stream);
}

RecvOperation::RecvOperation(
    int deviceIdx,
    CudaBuffer buffer,
    TRecvCallback callback)
    : ptr(buffer.ptr),
      length(buffer.length),
      deviceIdx(deviceIdx),
      stream(buffer.stream),
      callback(std::move(callback)) {}

void RecvOperation::process() {
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaStreamWaitEvent(stream, startEvent, 0));
    TP_CUDA_CHECK(
        cudaMemcpyAsync(ptr, srcPtr, length, cudaMemcpyDeviceToDevice, stream));
  }

  CudaEvent stopEv(deviceIdx);
  stopEv.record(stream);
  stopEv.wait(srcStream, srcDeviceIdx);
}

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> descriptorConnection,
    std::shared_ptr<transport::Connection> notificationConnection)
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      descriptorConnection_(std::move(descriptorConnection)),
      notificationConnection_(std::move(notificationConnection)) {}

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
      deviceIdx,
      buffer.unwrap<CudaBuffer>().ptr,
      buffer.unwrap<CudaBuffer>().stream,
      std::move(callback));
  SendOperation& op = *opIter;

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
      /*actions=*/{&ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the control connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::WRITING_DESCRIPTOR,
      /*cond=*/!error_ && prevOpState >= SendOperation::WRITING_DESCRIPTOR,
      /*actions=*/{&ChannelImpl::writeDescriptor});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::WRITING_DESCRIPTOR,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_ && op.doneWritingDescriptor,
      /*actions=*/{&ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on the control connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::WRITING_DESCRIPTOR,
      /*to=*/SendOperation::READING_NOTIFICATION,
      /*cond=*/!error_ && op.doneWritingDescriptor &&
          prevOpState >= SendOperation::READING_NOTIFICATION,
      /*actions=*/{&ChannelImpl::readNotification});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::READING_NOTIFICATION,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/op.doneReadingNotification,
      /*actions=*/{&ChannelImpl::callSendCallback});
}

void ChannelImpl::writeDescriptor(SendOpIter opIter) {
  SendOperation& op = *opIter;

  auto nopHolder = std::make_shared<NopHolder<Descriptor>>();
  Descriptor& nopDescriptor = nopHolder->getObject();
  static_assert(std::is_pointer<cudaEvent_t>::value, "");
  static_assert(std::is_pointer<cudaStream_t>::value, "");
  nopDescriptor.startEvent = reinterpret_cast<uintptr_t>(op.startEv.raw());
  nopDescriptor.srcDeviceIdx = op.deviceIdx;
  nopDescriptor.srcPtr = reinterpret_cast<uintptr_t>(op.ptr);
  nopDescriptor.srcStream = reinterpret_cast<uintptr_t>(op.stream);

  TP_VLOG(6) << "Channel " << id_ << " is writing descriptor (#"
             << op.sequenceNumber << ")";
  descriptorConnection_->write(
      *nopHolder, callbackWrapper_([opIter, nopHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done writing descriptor (#"
                   << opIter->sequenceNumber << ")";
        opIter->doneWritingDescriptor = true;
        impl.sendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::readNotification(SendOpIter opIter) {
  SendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading notification (#"
             << op.sequenceNumber << ")";
  notificationConnection_->read(
      nullptr,
      0,
      callbackWrapper_([opIter](
                           ChannelImpl& impl,
                           const void* /* unused */,
                           size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done reading notification (#"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingNotification = true;
        impl.sendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::callSendCallback(SendOpIter opIter) {
  SendOperation& op = *opIter;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
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
      buffer.unwrap<CudaBuffer>(),
      std::move(callback));

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

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on the control connection.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::READING_DESCRIPTOR,
      /*cond=*/!error_ && prevOpState >= RecvOperation::READING_DESCRIPTOR,
      /*actions=*/{&ChannelImpl::readDescriptor});

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::READING_DESCRIPTOR,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_ && op.doneReadingDescriptor,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the control connection.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::READING_DESCRIPTOR,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/!error_ && op.doneReadingDescriptor &&
          prevOpState >= RecvOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::waitOnStartEventAndCopyAndSyncWithSourceStream,
       &ChannelImpl::callRecvCallback,
       &ChannelImpl::writeNotification});
}

void ChannelImpl::readDescriptor(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading descriptor (#"
             << op.sequenceNumber << ")";
  auto nopHolderIn = std::make_shared<NopHolder<Descriptor>>();
  descriptorConnection_->read(
      *nopHolderIn, callbackWrapper_([opIter, nopHolderIn](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done reading descriptor (#"
                   << opIter->sequenceNumber << ")";
        Descriptor& nopDescriptor = nopHolderIn->getObject();
        static_assert(std::is_pointer<cudaEvent_t>::value, "");
        static_assert(std::is_pointer<cudaStream_t>::value, "");
        opIter->startEvent =
            reinterpret_cast<cudaEvent_t>(nopDescriptor.startEvent);
        opIter->srcPtr = reinterpret_cast<const void*>(nopDescriptor.srcPtr);
        opIter->srcDeviceIdx = nopDescriptor.srcDeviceIdx;
        opIter->srcStream =
            reinterpret_cast<cudaStream_t>(nopDescriptor.srcStream);

        opIter->doneReadingDescriptor = true;
        impl.recvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::waitOnStartEventAndCopyAndSyncWithSourceStream(
    RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#"
             << op.sequenceNumber << ")";
  op.process();
  TP_VLOG(6) << "Channel " << id_ << " done copying payload (#"
             << op.sequenceNumber << ")";
}

void ChannelImpl::callRecvCallback(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
}

void ChannelImpl::writeNotification(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is writing notification (#"
             << op.sequenceNumber << ")";
  notificationConnection_->write(
      nullptr,
      0,
      callbackWrapper_([sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done writing notification (#"
                   << sequenceNumber << ")";
      }));
}

void ChannelImpl::handleErrorImpl() {
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();

  descriptorConnection_->close();
  notificationConnection_->close();

  context_->unenroll(*this);
}

} // namespace cuda_xth
} // namespace channel
} // namespace tensorpipe
