/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/channel.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <limits>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cma {

Channel::Channel(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection)
    : impl_(Impl::create(std::move(context), std::move(connection))) {}

std::shared_ptr<Channel::Impl> Channel::Impl::create(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection) {
  auto impl = std::make_shared<Impl>(
      ConstructorToken(), std::move(context), std::move(connection));
  impl->init_();
  return impl;
}

Channel::Impl::Impl(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection)
    : context_(std::move(context)),
      connection_(std::move(connection)),
      closingReceiver_(context_, context_->getClosingEmitter()) {}

void Channel::Impl::init_() {
  deferToLoop_([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(inLoop_());
  closingReceiver_.activate(*this);
  readPacket_();
}

bool Channel::Impl::inLoop_() {
  return currentLoop_ == std::this_thread::get_id();
}

void Channel::Impl::deferToLoop_(std::function<void()> fn) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    pendingTasks_.push_back(std::move(fn));
    if (currentLoop_ != std::thread::id()) {
      return;
    }
    currentLoop_ = std::this_thread::get_id();
  }

  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (pendingTasks_.empty()) {
        currentLoop_ = std::thread::id();
        return;
      }
      task = std::move(pendingTasks_.front());
      pendingTasks_.pop_front();
    }
    task();
  }
}

void Channel::send(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  impl_->send(ptr, length, std::move(descriptorCallback), std::move(callback));
}

void Channel::Impl::send(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  deferToLoop_([this,
                ptr,
                length,
                descriptorCallback{std::move(descriptorCallback)},
                callback{std::move(callback)}]() mutable {
    sendFromLoop_(
        ptr, length, std::move(descriptorCallback), std::move(callback));
  });
}

void Channel::Impl::sendFromLoop_(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_DCHECK(inLoop_());
  // TP_THROW_ASSERT_IF(context_->joined_);
  if (error_) {
    // FIXME Ideally here we should either call the callback with an error (but
    // this may deadlock if we do it inline) or return an error as an additional
    // return value.
    TP_THROW_ASSERT();
  }
  proto::Descriptor pbDescriptor;

  const auto id = id_++;
  pbDescriptor.set_operation_id(id);
  pbDescriptor.set_pid(getpid());
  pbDescriptor.set_ptr(reinterpret_cast<uint64_t>(ptr));
  sendOperations_.emplace_back(SendOperation{id, std::move(callback)});

  descriptorCallback(Error::kSuccess, saveDescriptor(pbDescriptor));
}

// Receive memory region from peer.
void Channel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  impl_->recv(std::move(descriptor), ptr, length, std::move(callback));
}

void Channel::Impl::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  deferToLoop_([this,
                descriptor{std::move(descriptor)},
                ptr,
                length,
                callback{std::move(callback)}]() mutable {
    recvFromLoop_(std::move(descriptor), ptr, length, std::move(callback));
  });
}

void Channel::Impl::recvFromLoop_(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  TP_DCHECK(inLoop_());
  // TODO Short cut this if we're already in an error state.
  const auto pbDescriptor = loadDescriptor<proto::Descriptor>(descriptor);
  const uint64_t id = pbDescriptor.operation_id();
  pid_t remotePid = pbDescriptor.pid();
  void* remotePtr = reinterpret_cast<void*>(pbDescriptor.ptr());

  context_->requestCopy(
      remotePid,
      remotePtr,
      ptr,
      length,
      copyCallbackWrapper_([id, callback{std::move(callback)}](Impl& impl) {
        // Let peer know we've completed the copy.
        auto pbPacketOut = std::make_shared<proto::Packet>();
        proto::Notification* pbNotification =
            pbPacketOut->mutable_notification();
        pbNotification->set_operation_id(id);
        impl.connection_->write(
            *pbPacketOut,
            impl.writePacketCallbackWrapper_(
                [pbPacketOut](Impl& /* unused */) {}));
        callback(impl.error_);
      }));
}

void Channel::close() {
  impl_->close();
}

Channel::~Channel() {
  close();
}

void Channel::Impl::close() {
  deferToLoop_([this]() { closeFromLoop_(); });
}

void Channel::Impl::closeFromLoop_() {
  TP_DCHECK(inLoop_());
  if (!error_) {
    error_ = TP_CREATE_ERROR(ChannelClosedError);
    handleError_();
  }
}

void Channel::Impl::readPacket_() {
  TP_DCHECK(inLoop_());
  auto pbPacketIn = std::make_shared<proto::Packet>();
  connection_->read(
      *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
        impl.onPacket_(*pbPacketIn);
      }));
}

void Channel::Impl::onPacket_(const proto::Packet& pbPacketIn) {
  TP_DCHECK(inLoop_());

  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kNotification);
  onNotification_(pbPacketIn.notification());

  // Arm connection to wait for next packet.
  readPacket_();
}

void Channel::Impl::onNotification_(const proto::Notification& pbNotification) {
  TP_DCHECK(inLoop_());

  // Find the send operation matching the notification's operation ID.
  const auto id = pbNotification.operation_id();
  auto it = std::find_if(
      sendOperations_.begin(), sendOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == sendOperations_.end())
      << "Expected send operation with ID " << id << " to exist.";

  // Move operation to stack.
  auto op = std::move(*it);
  sendOperations_.erase(it);

  // Execute send completion callback.
  op.callback(Error::kSuccess);
}

void Channel::Impl::handleError_() {
  TP_DCHECK(inLoop_());

  // Move pending operations to stack.
  auto sendOperations = std::move(sendOperations_);

  // Notify pending send callbacks of error.
  for (auto& op : sendOperations) {
    op.callback(error_);
  }

  connection_->close();
}

} // namespace cma
} // namespace channel
} // namespace tensorpipe
