/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/xth/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/xth/context_impl.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace xth {

namespace {

struct Descriptor {
  uint64_t ptr;
  NOP_STRUCTURE(Descriptor, ptr);
};

} // namespace

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> descriptorConnection,
    std::shared_ptr<transport::Connection> completionConnection)
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      descriptorConnection_(std::move(descriptorConnection)),
      completionConnection_(std::move(completionConnection)) {}

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
  // of write calls on the descriptor control connection and read calls on the
  // completion control connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::READING_COMPLETION,
      /*cond=*/!error_ && prevOpState >= SendOperation::READING_COMPLETION,
      /*actions=*/
      {&ChannelImpl::writeDescriptor, &ChannelImpl::readCompletion});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::READING_COMPLETION,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/op.doneReadingCompletion,
      /*actions=*/{&ChannelImpl::callSendCallback});
}

void ChannelImpl::writeDescriptor(SendOpIter opIter) {
  SendOperation& op = *opIter;

  auto nopHolder = std::make_shared<NopHolder<Descriptor>>();
  Descriptor& nopDescriptor = nopHolder->getObject();
  nopDescriptor.ptr = reinterpret_cast<std::uintptr_t>(op.ptr);

  TP_VLOG(6) << "Channel " << id_ << " is writing descriptor (#"
             << op.sequenceNumber << ")";
  descriptorConnection_->write(
      *nopHolder,
      callbackWrapper_([sequenceNumber{op.sequenceNumber},
                        nopHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done writing descriptor (#"
                   << sequenceNumber << ")";
      }));
}

void ChannelImpl::readCompletion(SendOpIter opIter) {
  SendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading completion (#"
             << op.sequenceNumber << ")";
  completionConnection_->read(
      nullptr,
      0,
      callbackWrapper_([opIter](
                           ChannelImpl& impl,
                           const void* /* unused */,
                           size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done reading completion (#"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingCompletion = true;
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
  // of read calls on the descriptor control connection.
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

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::READING_DESCRIPTOR,
      /*to=*/RecvOperation::COPYING,
      /*cond=*/!error_ && op.doneReadingDescriptor,
      /*actions=*/{&ChannelImpl::copy});

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::COPYING,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_ && op.doneCopying,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the completion control connection.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::COPYING,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/!error_ && op.doneCopying &&
          prevOpState >= RecvOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::callRecvCallback, &ChannelImpl::writeCompletion});
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
        opIter->doneReadingDescriptor = true;
        if (!impl.error_) {
          Descriptor& nopDescriptor = nopHolderIn->getObject();
          opIter->remotePtr = reinterpret_cast<void*>(nopDescriptor.ptr);
        }
        impl.recvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::copy(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#"
             << op.sequenceNumber << ")";
  context_->requestCopy(
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

void ChannelImpl::writeCompletion(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is writing completion (#"
             << op.sequenceNumber << ")";
  completionConnection_->write(
      nullptr,
      0,
      callbackWrapper_([sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done writing completion (#"
                   << sequenceNumber << ")";
      }));
}

void ChannelImpl::handleErrorImpl() {
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();

  descriptorConnection_->close();
  completionConnection_->close();

  context_->unenroll(*this);
}

} // namespace xth
} // namespace channel
} // namespace tensorpipe
