/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <tensorpipe/channel/basic/context_impl.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace basic {

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
    size_t length,
    TSendCallback callback) {
  SendOpIter opIter = sendOps_.emplaceBack(sequenceNumber);
  SendOperation& op = *opIter;
  op.ptr = buffer.unwrap<CpuBuffer>().ptr;
  op.length = length;
  op.callback = std::move(callback);

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
      /*cond=*/error_ || op.length == 0,
      /*actions=*/{&ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::WRITING,
      /*cond=*/!error_ && prevOpState >= SendOperation::WRITING,
      /*actions=*/{&ChannelImpl::write});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::WRITING,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/op.doneWriting,
      /*actions=*/{&ChannelImpl::callSendCallback});
}

void ChannelImpl::write(SendOpIter opIter) {
  SendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is writing payload (#"
             << op.sequenceNumber << ")";
  connection_->write(
      op.ptr, op.length, callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done writing payload (#"
                   << opIter->sequenceNumber << ")";
        opIter->doneWriting = true;
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
    size_t length,
    TRecvCallback callback) {
  RecvOpIter opIter = recvOps_.emplaceBack(sequenceNumber);
  RecvOperation& op = *opIter;
  op.ptr = buffer.unwrap<CpuBuffer>().ptr;
  op.length = length;
  op.callback = std::move(callback);

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
      /*cond=*/error_ || op.length == 0,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on the connection.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::READING,
      /*cond=*/!error_ && prevOpState >= RecvOperation::READING,
      /*actions=*/{&ChannelImpl::read});

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::READING,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/op.doneReading,
      /*actions=*/{&ChannelImpl::callRecvCallback});
}

void ChannelImpl::read(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading payload (#"
             << op.sequenceNumber << ")";
  connection_->read(
      op.ptr,
      op.length,
      callbackWrapper_([opIter](
                           ChannelImpl& impl,
                           const void* /* unused */,
                           size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done reading payload (#"
                   << opIter->sequenceNumber << ")";
        opIter->doneReading = true;
        impl.recvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::callRecvCallback(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
}

void ChannelImpl::handleErrorImpl() {
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();

  // Close the connection so that all current operations will be aborted. This
  // will cause their callbacks to be invoked, and only then we'll invoke ours.
  connection_->close();

  context_->unenroll(*this);
}

} // namespace basic
} // namespace channel
} // namespace tensorpipe
