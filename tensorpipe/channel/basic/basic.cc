/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/basic.h>

#include <algorithm>

#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace channel {
namespace basic {

BasicChannelFactory::BasicChannelFactory()
    : ChannelFactory("basic"), domainDescriptor_("any") {}

BasicChannelFactory::~BasicChannelFactory() {}

const std::string& BasicChannelFactory::domainDescriptor() const {
  return domainDescriptor_;
}

std::shared_ptr<Channel> BasicChannelFactory::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint /* unused */) {
  auto channel = std::make_shared<BasicChannel>(
      BasicChannel::ConstructorToken(), std::move(connection));
  channel->init_();
  return channel;
}

BasicChannel::BasicChannel(
    ConstructorToken /* unused */,
    std::shared_ptr<transport::Connection> connection)
    : connection_(std::move(connection)) {
  // The factory calls `init_()` after construction so that we can use
  // `shared_from_this()`. The shared_ptr that refers to the object
  // itself isn't usable when the constructor is still being executed.
}

BasicChannel::TDescriptor BasicChannel::send(
    const void* ptr,
    size_t length,
    TSendCallback callback) {
  proto::Descriptor pbDescriptor;

  {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto id = id_++;
    pbDescriptor.set_operation_id(id);
    sendOperations_.emplace_back(
        SendOperation{id, ptr, length, std::move(callback)});
  }

  return saveDescriptor(pbDescriptor);
}

// Receive memory region from peer.
void BasicChannel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  const auto pbDescriptor = loadDescriptor<proto::Descriptor>(descriptor);
  const auto id = pbDescriptor.operation_id();

  {
    std::unique_lock<std::mutex> lock(mutex_);
    recvOperations_.emplace_back(
        RecvOperation{id, ptr, length, std::move(callback)});
  }

  // Ask peer to start sending data now that we have a target pointer.
  auto packet = std::make_shared<proto::Packet>();
  proto::Request* pbRequest = packet->mutable_request();
  pbRequest->set_operation_id(id);
  connection_->write(
      *packet,
      wrapWriteCallback_(
          [packet](BasicChannel& /* unused */, TLock /* unused */) {}));
  return;
}

void BasicChannel::init_() {
  std::unique_lock<std::mutex> lock(mutex_);
  readPacket_(lock);
}

void BasicChannel::readPacket_(TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto packet = std::make_shared<proto::Packet>();
  connection_->read(
      *packet,
      wrapReadProtoCallback_([packet](BasicChannel& channel, TLock lock) {
        channel.onPacket_(*packet, lock);
      }));
}

void BasicChannel::onPacket_(const proto::Packet& packet, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  if (packet.has_request()) {
    onRequest_(packet.request(), lock);
  } else if (packet.has_reply()) {
    onReply_(packet.reply(), lock);
  } else {
    TP_THROW_ASSERT() << "Packet is not a request nor a reply.";
  }

  // Wait for next request.
  readPacket_(lock);
}

void BasicChannel::onRequest_(const proto::Request& request, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
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
      wrapWriteCallback_(
          [pbPacketOut](BasicChannel& /* unused */, TLock /* unused */) {}));

  // Write payload.
  connection_->write(
      op.ptr,
      op.length,
      wrapWriteCallback_([id](BasicChannel& channel, TLock lock) {
        channel.sendCompleted(id, lock);
      }));
}

void BasicChannel::onReply_(const proto::Reply& reply, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
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
      wrapReadCallback_([id](
                            BasicChannel& channel,
                            const void* /* unused */,
                            size_t /* unused */,
                            TLock lock) { channel.recvCompleted(id, lock); }));
}

void BasicChannel::sendCompleted(const uint64_t id, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto it = std::find_if(
      sendOperations_.begin(), sendOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == sendOperations_.end())
      << "Expected send operation with ID " << id << " to exist.";

  // Move operation to stack.
  auto op = std::move(*it);
  sendOperations_.erase(it);

  // Release lock before executing callback.
  lock.unlock();
  op.callback(Error::kSuccess);
}

void BasicChannel::recvCompleted(const uint64_t id, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto it = std::find_if(
      recvOperations_.begin(), recvOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == recvOperations_.end())
      << "Expected recv operation with ID " << id << " to exist.";

  // Move operation to stack.
  auto op = std::move(*it);
  recvOperations_.erase(it);

  // Release lock before executing callback.
  lock.unlock();
  op.callback(Error::kSuccess);
}

BasicChannel::TReadCallback BasicChannel::wrapReadCallback_(
    TBoundReadCallback fn) {
  return runIfAlive(
      *this,
      std::function<void(BasicChannel&, const Error&, const void*, size_t)>(
          [fn{std::move(fn)}](
              BasicChannel& channel,
              const Error& error,
              const void* ptr,
              size_t length) {
            channel.readCallbackEntryPoint_(error, ptr, length, std::move(fn));
          }));
}

BasicChannel::TReadProtoCallback BasicChannel::wrapReadProtoCallback_(
    TBoundReadProtoCallback fn) {
  return runIfAlive(
      *this,
      std::function<void(BasicChannel&, const Error&)>(
          [fn{std::move(fn)}](BasicChannel& channel, const Error& error) {
            channel.readProtoCallbackEntryPoint_(error, std::move(fn));
          }));
}

BasicChannel::TWriteCallback BasicChannel::wrapWriteCallback_(
    TBoundWriteCallback fn) {
  return runIfAlive(
      *this,
      std::function<void(BasicChannel&, const Error&)>(
          [fn{std::move(fn)}](BasicChannel& channel, const Error& error) {
            channel.writeCallbackEntryPoint_(error, std::move(fn));
          }));
}

void BasicChannel::readCallbackEntryPoint_(
    const Error& error,
    const void* ptr,
    size_t length,
    TBoundReadCallback fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (processError(error, lock)) {
    return;
  }
  if (fn) {
    fn(*this, ptr, length, lock);
  }
}

void BasicChannel::readProtoCallbackEntryPoint_(
    const Error& error,
    TBoundReadProtoCallback fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (processError(error, lock)) {
    return;
  }
  if (fn) {
    fn(*this, lock);
  }
}

void BasicChannel::writeCallbackEntryPoint_(
    const Error& error,
    TBoundWriteCallback fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (processError(error, lock)) {
    return;
  }
  if (fn) {
    fn(*this, lock);
  }
}

bool BasicChannel::processError(const Error& error, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);

  // Ignore if an error was already set.
  if (error_) {
    return true;
  }

  // If this is the first callback with an error, make sure that all
  // pending user specified callbacks get called with that same error.
  // Once the channel is in an error state it doesn't recover.
  if (error) {
    error_ = error;

    // Move pending operations to stack.
    auto sendOperations = std::move(sendOperations_);
    auto recvOperations = std::move(recvOperations_);

    // Release lock before executing callbacks.
    lock.unlock();

    // Notify pending send callbacks of error.
    for (auto& op : sendOperations) {
      op.callback(error_);
    }

    // Notify pending recv callbacks of error.
    for (auto& op : recvOperations) {
      op.callback(error_);
    }

    return true;
  }

  return false;
}

} // namespace basic
} // namespace channel
} // namespace tensorpipe
