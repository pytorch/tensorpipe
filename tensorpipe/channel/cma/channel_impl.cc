/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cma/context_impl.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cma {

namespace {

struct Descriptor {
  uint32_t pid;
  uint64_t ptr;
  NOP_STRUCTURE(Descriptor, pid, ptr);
};

} // namespace

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection)
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  SendOpIter opIter = sendOps_.emplaceBack(sequenceNumber);
  SendOperation& op = *opIter;
  op.callback = std::move(callback);

  sendOps_.advanceOperation(opIter);

  NopHolder<Descriptor> nopHolder;
  Descriptor& nopDescriptor = nopHolder.getObject();
  nopDescriptor.pid = ::getpid();
  nopDescriptor.ptr =
      reinterpret_cast<uint64_t>(buffer.unwrap<CpuBuffer>().ptr);
  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
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
  // of read calls on the control connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::READING_NOTIFICATION,
      /*cond=*/!error_ && prevOpState >= SendOperation::READING_NOTIFICATION,
      /*actions=*/{&ChannelImpl::readNotification});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::READING_NOTIFICATION,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/op.doneReadingNotification,
      /*actions=*/{&ChannelImpl::callSendCallback});
}

void ChannelImpl::readNotification(SendOpIter opIter) {
  SendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading notification (#"
             << op.sequenceNumber << ")";
  connection_->read(
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
    TDescriptor descriptor,
    Buffer buffer,
    TRecvCallback callback) {
  RecvOpIter opIter = recvOps_.emplaceBack(sequenceNumber);
  RecvOperation& op = *opIter;
  op.ptr = buffer.unwrap<CpuBuffer>().ptr;
  op.length = buffer.unwrap<CpuBuffer>().length;
  op.callback = std::move(callback);

  NopHolder<Descriptor> nopHolder;
  loadDescriptor(nopHolder, descriptor);
  Descriptor& nopDescriptor = nopHolder.getObject();
  op.remotePid = nopDescriptor.pid;
  op.remotePtr = reinterpret_cast<void*>(nopDescriptor.ptr);

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

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::COPYING,
      /*cond=*/!error_,
      /*actions=*/{&ChannelImpl::copy});

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::COPYING,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_ && op.doneCopying,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the control connection.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::COPYING,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/!error_ && op.doneCopying &&
          prevOpState >= RecvOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::callRecvCallback, &ChannelImpl::writeNotification});
}

void ChannelImpl::copy(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#"
             << op.sequenceNumber << ")";
  context_->requestCopy(
      op.remotePid,
      op.remotePtr,
      op.ptr,
      op.length,
      callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done copying payload (#"
                   << opIter->sequenceNumber << ")";
        opIter->doneCopying = true;
        impl.recvOps_.advanceOperation(opIter);
      }));
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
  connection_->write(
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

  connection_->close();

  context_->unenroll(*this);
}

} // namespace cma
} // namespace channel
} // namespace tensorpipe
