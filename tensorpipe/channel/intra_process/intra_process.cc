/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/intra_process/intra_process.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace intra_process {

namespace {

const std::string kChannelName{"intra_process"};

std::string generateDomainDescriptor() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  // Combine boot ID, effective UID, and effective GID.
  oss << kChannelName;
  oss << ":" << bootID.value();
  oss << "/" << getpid();
  return oss.str();
}

} // namespace

IntraProcessChannel::IntraProcessChannel(
    ConstructorToken /* unused */,
    std::shared_ptr<IntraProcessChannelFactory> factory,
    std::shared_ptr<transport::Connection> connection)
    : factory_(std::move(factory)), connection_(std::move(connection)) {
  // The factory calls `init_()` after construction so that we can use
  // `shared_from_this()`. The shared_ptr that refers to the object
  // itself isn't usable when the constructor is still being executed.
}

void IntraProcessChannel::init_() {
  readPacket_();
}

IntraProcessChannel::TDescriptor IntraProcessChannel::send(
    const void* ptr,
    size_t length,
    TSendCallback callback) {
  TP_THROW_ASSERT_IF(factory_->joined_);
  if (error_) {
    // FIXME Ideally here we should either call the callback with an error (but
    // this may deadlock if we do it inline) or return an error as an additional
    // return value.
    TP_THROW_ASSERT();
  }
  proto::Descriptor pbDescriptor;

  {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto id = id_++;
    pbDescriptor.set_operation_id(id);
    pbDescriptor.set_ptr(reinterpret_cast<uint64_t>(ptr));
    sendOperations_.emplace_back(SendOperation{id, std::move(callback)});
  }

  return saveDescriptor(pbDescriptor);
}

// Receive memory region from peer.
void IntraProcessChannel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  // TODO Short cut this if we're already in an error state.
  const auto pbDescriptor = loadDescriptor<proto::Descriptor>(descriptor);
  const uint64_t id = pbDescriptor.operation_id();
  void* remotePtr = reinterpret_cast<void*>(pbDescriptor.ptr());

  factory_->requestCopy_(
      remotePtr,
      ptr,
      length,
      runIfAlive(
          *this,
          std::function<void(IntraProcessChannel&, const Error&)>(
              [id, callback{std::move(callback)}](
                  IntraProcessChannel& channel, const Error& error) {
                if (channel.processError(error)) {
                  return;
                }
                // Let peer know we've completed the copy.
                proto::Packet pbPacketOut;
                proto::Notification* pbNotification =
                    pbPacketOut.mutable_notification();
                pbNotification->set_operation_id(id);
                channel.connection_->write(
                    pbPacketOut, channel.wrapWriteCallback_());

                callback(Error::kSuccess);
              })));
}

void IntraProcessChannel::readPacket_() {
  connection_->read(wrapReadProtoCallback_(
      [](IntraProcessChannel& channel, const proto::Packet& pbPacketIn) {
        channel.onPacket_(pbPacketIn);
      }));
}

void IntraProcessChannel::onPacket_(const proto::Packet& pbPacketIn) {
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kNotification);
  onNotification_(pbPacketIn.notification());

  // Arm connection to wait for next packet.
  readPacket_();
}

void IntraProcessChannel::onNotification_(
    const proto::Notification& pbNotification) {
  std::unique_lock<std::mutex> lock(mutex_);

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

  // Release lock before executing callback.
  lock.unlock();

  // Execute send completion callback.
  op.callback(Error::kSuccess);
}

IntraProcessChannel::TReadProtoCallback IntraProcessChannel::
    wrapReadProtoCallback_(TBoundReadProtoCallback fn) {
  return runIfAlive(
      *this,
      std::function<void(
          IntraProcessChannel&, const Error&, const proto::Packet&)>(
          [fn{std::move(fn)}](
              IntraProcessChannel& channel,
              const Error& error,
              const proto::Packet& packet) {
            channel.readProtoCallbackEntryPoint_(error, packet, std::move(fn));
          }));
}

IntraProcessChannel::TWriteCallback IntraProcessChannel::wrapWriteCallback_(
    TBoundWriteCallback fn) {
  return runIfAlive(
      *this,
      std::function<void(IntraProcessChannel&, const Error&)>(
          [fn{std::move(fn)}](
              IntraProcessChannel& channel, const Error& error) {
            channel.writeCallbackEntryPoint_(error, std::move(fn));
          }));
}

void IntraProcessChannel::readProtoCallbackEntryPoint_(
    const Error& error,
    const proto::Packet& packet,
    TBoundReadProtoCallback fn) {
  if (processError(error)) {
    return;
  }
  if (fn) {
    fn(*this, packet);
  }
}

void IntraProcessChannel::writeCallbackEntryPoint_(
    const Error& error,
    TBoundWriteCallback fn) {
  if (processError(error)) {
    return;
  }
  if (fn) {
    fn(*this);
  }
}

bool IntraProcessChannel::processError(const Error& error) {
  std::unique_lock<std::mutex> lock(mutex_);

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

    // Release lock before executing callbacks.
    lock.unlock();

    // Notify pending send callbacks of error.
    for (auto& op : sendOperations) {
      op.callback(error_);
    }

    return true;
  }

  return false;
}

IntraProcessChannelFactory::IntraProcessChannelFactory()
    : ChannelFactory(kChannelName),
      domainDescriptor_(generateDomainDescriptor()) {
  thread_ = std::thread(&IntraProcessChannelFactory::handleCopyRequests_, this);
}

IntraProcessChannelFactory::~IntraProcessChannelFactory() {
  if (!joined_) {
    TP_LOG_WARNING()
        << "The channel factory is being destroyed but join() wasn't called on "
        << "it. Perhaps a scope exited prematurely, possibly due to an "
        << "exception?";
    join();
  }
  TP_DCHECK(joined_);
  TP_DCHECK(!thread_.joinable());
}

const std::string& IntraProcessChannelFactory::domainDescriptor() const {
  return domainDescriptor_;
}

std::shared_ptr<Channel> IntraProcessChannelFactory::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint /* unused */) {
  TP_THROW_ASSERT_IF(joined_);
  auto channel = std::make_shared<IntraProcessChannel>(
      IntraProcessChannel::ConstructorToken(),
      shared_from_this(),
      std::move(connection));
  channel->init_();
  return channel;
}

void IntraProcessChannelFactory::requestCopy_(
    void* remotePtr,
    void* localPtr,
    size_t length,
    copy_request_callback_fn fn) {
  requests_.push(CopyRequest{remotePtr, localPtr, length, std::move(fn)});
}

void IntraProcessChannelFactory::handleCopyRequests_() {
  while (true) {
    auto maybeRequest = requests_.pop();
    if (!maybeRequest.has_value()) {
      break;
    }
    CopyRequest request = std::move(maybeRequest).value();

    // Perform copy.
    auto nread = ::memcpy(request.localPtr, request.remotePtr, request.length);
    if (nread == -1) {
      request.callback(TP_CREATE_ERROR(SystemError, "intra_process", errno));
    } else if (nread != request.length) {
      request.callback(TP_CREATE_ERROR(ShortReadError, request.length, nread));
    } else {
      request.callback(Error::kSuccess);
    }
  }
}

void IntraProcessChannelFactory::join() {
  joined_ = true;
  requests_.push(nullopt);
  thread_.join();
}

} // namespace intra_process
} // namespace channel
} // namespace tensorpipe