/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/basic.h>

#include <algorithm>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {
namespace channel {
namespace basic {

Context::Context()
    : channel::Context("basic"), impl_(std::make_shared<Impl>()) {}

Context::Impl::Impl() : domainDescriptor_("any") {}

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
}

std::shared_ptr<channel::Channel> Context::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint endpoint) {
  return impl_->createChannel(std::move(connection), endpoint);
}

std::shared_ptr<channel::Channel> Context::Impl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint /* unused */) {
  return std::make_shared<Channel>(
      Channel::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(connection));
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  bool wasClosed = false;
  closed_.compare_exchange_strong(wasClosed, true);
  if (!wasClosed) {
    closingEmitter_.close();
  }
}

void Context::join() {
  impl_->join();
}

void Context::Impl::join() {
  close();

  bool wasJoined = false;
  joined_.compare_exchange_strong(wasJoined, true);
  if (!wasJoined) {
    // Nothing to do?
  }
}

Context::~Context() {
  join();
}

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
      closingReceiver_(context_, context_->getClosingEmitter()),
      readCallbackWrapper_(*this),
      readProtoCallbackWrapper_(*this),
      writeCallbackWrapper_(*this),
      writeProtoCallbackWrapper_(*this) {}

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

// Send memory region to peer.
void Channel::Impl::sendFromLoop_(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_DCHECK(inLoop_());
  proto::Descriptor pbDescriptor;

  const auto id = id_++;
  pbDescriptor.set_operation_id(id);
  sendOperations_.emplace_back(
      SendOperation{id, ptr, length, std::move(callback)});

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
  const auto pbDescriptor = loadDescriptor<proto::Descriptor>(descriptor);
  const auto id = pbDescriptor.operation_id();

  recvOperations_.emplace_back(
      RecvOperation{id, ptr, length, std::move(callback)});

  // Ask peer to start sending data now that we have a target pointer.
  auto packet = std::make_shared<proto::Packet>();
  proto::Request* pbRequest = packet->mutable_request();
  pbRequest->set_operation_id(id);
  connection_->write(
      *packet, writeProtoCallbackWrapper_([packet](Impl& /* unused */) {}));
  return;
}

void Channel::Impl::init_() {
  deferToLoop_([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(inLoop_());
  closingReceiver_.activate(*this);
  readPacket_();
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
  auto packet = std::make_shared<proto::Packet>();
  connection_->read(*packet, readProtoCallbackWrapper_([packet](Impl& impl) {
    impl.onPacket_(*packet);
  }));
}

void Channel::Impl::onPacket_(const proto::Packet& packet) {
  TP_DCHECK(inLoop_());
  if (packet.has_request()) {
    onRequest_(packet.request());
  } else if (packet.has_reply()) {
    onReply_(packet.reply());
  } else {
    TP_THROW_ASSERT() << "Packet is not a request nor a reply.";
  }

  // Wait for next request.
  readPacket_();
}

void Channel::Impl::onRequest_(const proto::Request& request) {
  TP_DCHECK(inLoop_());
  // Find the send operation matching the request's operation ID.
  const auto id = request.operation_id();
  auto it = std::find_if(
      sendOperations_.begin(), sendOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == sendOperations_.end())
      << "Expected send operation with ID " << id << " to exist.";

  // Reference to operation.
  auto& op = *it;

  // Write packet announcing the payload.
  auto pbPacketOut = std::make_shared<proto::Packet>();
  proto::Reply* pbReply = pbPacketOut->mutable_reply();
  pbReply->set_operation_id(id);
  connection_->write(
      *pbPacketOut,
      writeProtoCallbackWrapper_([pbPacketOut](Impl& /* unused */) {}));

  // Write payload.
  connection_->write(op.ptr, op.length, writeCallbackWrapper_([id](Impl& impl) {
                       impl.sendCompleted(id);
                     }));
}

void Channel::Impl::onReply_(const proto::Reply& reply) {
  TP_DCHECK(inLoop_());
  // Find the recv operation matching the reply's operation ID.
  const auto id = reply.operation_id();
  auto it = std::find_if(
      recvOperations_.begin(), recvOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == recvOperations_.end())
      << "Expected recv operation with ID " << id << " to exist.";

  // Reference to operation.
  auto& op = *it;

  // Read payload into specified memory region.
  connection_->read(
      op.ptr,
      op.length,
      readCallbackWrapper_(
          [id](Impl& impl, const void* /* unused */, size_t /* unused */) {
            impl.recvCompleted(id);
          }));
}

void Channel::Impl::sendCompleted(const uint64_t id) {
  TP_DCHECK(inLoop_());
  auto it = std::find_if(
      sendOperations_.begin(), sendOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == sendOperations_.end())
      << "Expected send operation with ID " << id << " to exist.";

  // Move operation to stack.
  auto op = std::move(*it);
  sendOperations_.erase(it);

  op.callback(error_);
}

void Channel::Impl::recvCompleted(const uint64_t id) {
  TP_DCHECK(inLoop_());
  auto it = std::find_if(
      recvOperations_.begin(), recvOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == recvOperations_.end())
      << "Expected recv operation with ID " << id << " to exist.";

  // Move operation to stack.
  auto op = std::move(*it);
  recvOperations_.erase(it);

  op.callback(error_);
}

void Channel::Impl::handleError_() {
  TP_DCHECK(inLoop_());
  // Close the connection so that all current operations will be aborted. This
  // will cause their callbacks to be invoked, and only then we'll invoke ours.
  connection_->close();
}

} // namespace basic
} // namespace channel
} // namespace tensorpipe
