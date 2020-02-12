/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/process_vm_readv.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>

#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/system.h>
#include <tensorpipe/core/error_macros.h>

namespace tensorpipe {

namespace {

const std::string kChannelName{"process_vm_readv"};

std::string generateDomainDescriptor() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";

  // Combine boot ID, effective UID, and effective GID.
  oss << kChannelName;
  oss << ":" << bootID.value();
  oss << "/" << geteuid();
  oss << "/" << getegid();
  return oss.str();
}

} // namespace

ProcessVmReadvChannelFactory::ProcessVmReadvChannelFactory()
    : ChannelFactory(kChannelName),
      domainDescriptor_(generateDomainDescriptor()) {}

ProcessVmReadvChannelFactory::~ProcessVmReadvChannelFactory() {}

const std::string& ProcessVmReadvChannelFactory::domainDescriptor() const {
  return domainDescriptor_;
}

std::shared_ptr<Channel> ProcessVmReadvChannelFactory::createChannel(
    std::shared_ptr<transport::Connection> connection) {
  auto channel = std::make_shared<ProcessVmReadvChannel>(
      ProcessVmReadvChannel::ConstructorToken(), std::move(connection));
  channel->init_();
  return channel;
}

ProcessVmReadvChannel::ProcessVmReadvChannel(
    ConstructorToken /* unused */,
    std::shared_ptr<transport::Connection> connection)
    : connection_(std::move(connection)) {
  // The factory calls `init_()` after construction so that we can use
  // `shared_from_this()`. The shared_ptr that refers to the object
  // itself isn't usable when the constructor is still being executed.
}

ProcessVmReadvChannel::TDescriptor ProcessVmReadvChannel::send(
    const void* ptr,
    size_t length,
    TSendCallback callback) {
  proto::ProcessVmReadvChannelOperation op;

  {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto id = id_++;
    op.set_operation_id(id);
    op.set_pid(getpid());
    op.set_iov_base(reinterpret_cast<uint64_t>(ptr));
    op.set_iov_len(length);
    sendOperations_.emplace_back(SendOperation{id, std::move(callback)});
  }

  return saveDescriptor(op);
}

// Receive memory region from peer.
void ProcessVmReadvChannel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  const auto op =
      loadDescriptor<proto::ProcessVmReadvChannelOperation>(descriptor);
  const auto id = op.operation_id();

  // The descriptor encodes the size in bytes for this operation. Make
  // sure that the size the sender will send is equal to the size of
  // the memory region passed to this function. If they are not, this
  // is a programming error.
  TP_THROW_ASSERT_IF(length != op.iov_len())
      << ": recv was called with length " << length
      << ", whereas the descriptor encoded length " << op.iov_len() << ".";

  // Perform copy.
  struct iovec local {
    .iov_base = ptr, .iov_len = length
  };
  struct iovec remote {
    .iov_base = reinterpret_cast<void*>(op.iov_base()), .iov_len = op.iov_len()
  };
  auto nread = process_vm_readv(op.pid(), &local, 1, &remote, 1, 0);
  TP_THROW_ASSERT_IF(length != op.iov_len());

  // Let peer know we've completed the copy.
  proto::ProcessVmReadvChannelPacket packet;
  *packet.mutable_notification() = op;
  connection_->write(packet, wrapWriteCallback_());
  return;
}

void ProcessVmReadvChannel::init_() {
  readPacket_();
}

void ProcessVmReadvChannel::readPacket_() {
  connection_->read(wrapReadProtoCallback_(
      [](ProcessVmReadvChannel& channel,
         const proto::ProcessVmReadvChannelPacket& packet) {
        channel.onPacket_(packet);
      }));
}

void ProcessVmReadvChannel::onPacket_(
    const proto::ProcessVmReadvChannelPacket& packet) {
  if (packet.has_notification()) {
    onNotification_(packet.notification());
  } else {
    TP_THROW_ASSERT() << "Packet type not handled.";
  }
}

void ProcessVmReadvChannel::onNotification_(
    const proto::ProcessVmReadvChannelOperation& notification) {
  std::unique_lock<std::mutex> lock(mutex_);

  // Find the send operation matching the notification's operation ID.
  const auto id = notification.operation_id();
  auto it = std::find_if(
      sendOperations_.begin(), sendOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == sendOperations_.end())
      << "Expected send operation with ID " << id << " to exist.";

  // Move operation to stack.
  auto op = std::move(*it);
  sendOperations_.erase(it);

  // Arm connection to wait for next packet.
  readPacket_();

  // Release lock before executing callback.
  lock.unlock();

  // Execute send completion callback.
  op.callback();
}

ProcessVmReadvChannel::TReadProtoCallback ProcessVmReadvChannel::
    wrapReadProtoCallback_(TBoundReadProtoCallback fn) {
  return runIfAlive(
      *this,
      std::function<void(
          ProcessVmReadvChannel&,
          const transport::Error&,
          const proto::ProcessVmReadvChannelPacket&)>(
          [fn{std::move(fn)}](
              ProcessVmReadvChannel& channel,
              const transport::Error& error,
              const proto::ProcessVmReadvChannelPacket& packet) {
            channel.readProtoCallbackEntryPoint_(error, packet, std::move(fn));
          }));
}

ProcessVmReadvChannel::TWriteCallback ProcessVmReadvChannel::wrapWriteCallback_(
    TBoundWriteCallback fn) {
  return runIfAlive(
      *this,
      std::function<void(ProcessVmReadvChannel&, const transport::Error&)>(
          [fn{std::move(fn)}](
              ProcessVmReadvChannel& channel, const transport::Error& error) {
            channel.writeCallbackEntryPoint_(error, std::move(fn));
          }));
}

void ProcessVmReadvChannel::readProtoCallbackEntryPoint_(
    const transport::Error& error,
    const proto::ProcessVmReadvChannelPacket& packet,
    TBoundReadProtoCallback fn) {
  if (processTransportError(error)) {
    return;
  }
  if (fn) {
    fn(*this, packet);
  }
}

void ProcessVmReadvChannel::writeCallbackEntryPoint_(
    const transport::Error& error,
    TBoundWriteCallback fn) {
  if (processTransportError(error)) {
    return;
  }
  if (fn) {
    fn(*this);
  }
}

bool ProcessVmReadvChannel::processTransportError(
    const transport::Error& error) {
  std::unique_lock<std::mutex> lock(mutex_);

  // Ignore if an error was already set.
  if (error_) {
    return true;
  }

  // If this is the first callback with an error, make sure that all
  // pending user specified callbacks get called with that same error.
  // Once the channel is in an error state it doesn't recover.
  if (error) {
    error_ = TP_CREATE_ERROR(TransportError, error);

    // Move pending operations to stack.
    auto sendOperations = std::move(sendOperations_);

    // Release lock before executing callbacks.
    lock.unlock();

    // Notify pending send callbacks of error.
    for (auto& op : sendOperations) {
      // TODO(pietern): pass error.
      op.callback();
    }

    return true;
  }

  return false;
}

} // namespace tensorpipe
