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
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  SendOpIter opIter = sendOps_.emplaceBack(sequenceNumber);
  SendOperation& op = *opIter;
  op.callback = std::move(callback);
  op.ptr = buffer.unwrap<CpuBuffer>().ptr;

  sendOps_.advanceOperation(opIter);
  descriptorCallback(Error::kSuccess, "");
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
  // TODO: Store the PID upon channel/context instantiation.
  nopDescriptor.pid = ::getpid();
  nopDescriptor.ptr = reinterpret_cast<uint64_t>(op.ptr);

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
    TDescriptor descriptor,
    Buffer buffer,
    TRecvCallback callback) {
  TP_DCHECK_EQ(descriptor, "");
  RecvOpIter opIter = recvOps_.emplaceBack(sequenceNumber);
  RecvOperation& op = *opIter;
  op.ptr = buffer.unwrap<CpuBuffer>().ptr;
  op.length = buffer.unwrap<CpuBuffer>().length;
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
        opIter->remotePid = nopDescriptor.pid;
        opIter->remotePtr = reinterpret_cast<void*>(nopDescriptor.ptr);

        opIter->doneReadingDescriptor = true;
        impl.recvOps_.advanceOperation(opIter);
      }));
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

} // namespace cma
} // namespace channel
} // namespace tensorpipe
