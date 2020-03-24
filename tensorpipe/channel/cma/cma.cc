/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/cma.h>

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
namespace cma {

namespace {

const std::string kChannelName{"cma"};

std::string generateDomainDescriptor() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";

  // According to the man page of process_vm_readv and process_vm_writev,
  // permission to read from or write to another process is governed by a ptrace
  // access mode PTRACE_MODE_ATTACH_REALCREDS check. This consists in a series
  // of checks, some governed by the CAP_SYS_PTRACE capability, others by the
  // Linux Security Modules (LSMs), but the primary constraint is that the real,
  // effective, and saved-set user IDs of the target match the caller's real
  // user ID, and the same for group IDs. Since channels are bidirectional, we
  // end up needing these IDs to all be the same on both processes.

  // Combine boot ID, effective UID, and effective GID.
  oss << kChannelName;
  oss << ":" << bootID.value();
  // FIXME As domain descriptors are just compared for equality, we only include
  // the effective IDs, but we should abide by the rules above and make sure
  // that they match the real and saved-set ones too.
  oss << "/" << geteuid();
  oss << "/" << getegid();
  return oss.str();
}

} // namespace

std::shared_ptr<CmaChannelFactory> CmaChannelFactory::create() {
  auto factory = std::make_shared<CmaChannelFactory>(ConstructorToken());
  factory->init_();
  return factory;
}

CmaChannelFactory::CmaChannelFactory(
    ConstructorToken /* unused */,
    int queueCapacity)
    : ChannelFactory(kChannelName),
      domainDescriptor_(generateDomainDescriptor()),
      requests_(queueCapacity) {}

void CmaChannelFactory::init_() {
  std::unique_lock<std::mutex> lock(mutex_);
  thread_ = std::thread(&CmaChannelFactory::handleCopyRequests_, this);
}

void CmaChannelFactory::join() {
  std::unique_lock<std::mutex> lock(mutex_);
  TP_THROW_ASSERT_IF(joined_);
  joined_ = true;
  requests_.push(nullopt);
  thread_.join();
}

CmaChannelFactory::~CmaChannelFactory() {
  if (!joined_) {
    TP_LOG_WARNING()
        << "The channel factory is being destroyed but join() wasn't called on "
        << "it. Perhaps a scope exited prematurely, possibly due to an "
        << "exception?";
    join();
  }
  TP_DCHECK(joined_);
  // TP_DCHECK(requests_.empty());
  TP_DCHECK(!thread_.joinable());
}

const std::string& CmaChannelFactory::domainDescriptor() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return domainDescriptor_;
}

std::shared_ptr<Channel> CmaChannelFactory::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint /* unused */) {
  TP_THROW_ASSERT_IF(joined_);
  auto channel = std::make_shared<CmaChannel>(
      CmaChannel::ConstructorToken(),
      shared_from_this(),
      std::move(connection));
  channel->init_();
  return channel;
}

void CmaChannelFactory::requestCopy_(
    pid_t remotePid,
    void* remotePtr,
    void* localPtr,
    size_t length,
    std::function<void(const Error&)> fn) {
  requests_.push(
      CopyRequest{remotePid, remotePtr, localPtr, length, std::move(fn)});
}

void CmaChannelFactory::handleCopyRequests_() {
  while (true) {
    auto maybeRequest = requests_.pop();
    if (!maybeRequest.has_value()) {
      break;
    }
    CopyRequest request = std::move(maybeRequest).value();

    // Perform copy.
    struct iovec local {
      .iov_base = request.localPtr, .iov_len = request.length
    };
    struct iovec remote {
      .iov_base = request.remotePtr, .iov_len = request.length
    };
    auto nread =
        ::process_vm_readv(request.remotePid, &local, 1, &remote, 1, 0);
    if (nread == -1) {
      request.callback(TP_CREATE_ERROR(SystemError, "cma", errno));
    } else if (nread != request.length) {
      request.callback(TP_CREATE_ERROR(ShortReadError, request.length, nread));
    } else {
      request.callback(Error::kSuccess);
    }
  }
}

CmaChannel::CmaChannel(
    ConstructorToken /* unused */,
    std::shared_ptr<CmaChannelFactory> factory,
    std::shared_ptr<transport::Connection> connection)
    : factory_(std::move(factory)), connection_(std::move(connection)) {
  // The factory calls `init_()` after construction so that we can use
  // `shared_from_this()`. The shared_ptr that refers to the object
  // itself isn't usable when the constructor is still being executed.
}

void CmaChannel::init_() {
  readPacket_();
}

void CmaChannel::send(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
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
    pbDescriptor.set_pid(getpid());
    pbDescriptor.set_ptr(reinterpret_cast<uint64_t>(ptr));
    sendOperations_.emplace_back(SendOperation{id, std::move(callback)});
  }

  descriptorCallback(Error::kSuccess, saveDescriptor(pbDescriptor));
}

// Receive memory region from peer.
void CmaChannel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  // TODO Short cut this if we're already in an error state.
  const auto pbDescriptor = loadDescriptor<proto::Descriptor>(descriptor);
  const uint64_t id = pbDescriptor.operation_id();
  pid_t remotePid = pbDescriptor.pid();
  void* remotePtr = reinterpret_cast<void*>(pbDescriptor.ptr());

  factory_->requestCopy_(
      remotePid,
      remotePtr,
      ptr,
      length,
      copyCallbackWrapper_(
          [id, callback{std::move(callback)}](CmaChannel& channel, TLock lock) {
            // Let peer know we've completed the copy.
            auto pbPacketOut = std::make_shared<proto::Packet>();
            proto::Notification* pbNotification =
                pbPacketOut->mutable_notification();
            pbNotification->set_operation_id(id);
            channel.connection_->write(
                *pbPacketOut,
                channel.writeCallbackWrapper_(
                    [pbPacketOut](
                        CmaChannel& /* unused */, TLock /* unused */) {}));
            lock.unlock();
            callback(Error::kSuccess);
          }));
}

void CmaChannel::readPacket_() {
  auto pbPacketIn = std::make_shared<proto::Packet>();
  connection_->read(
      *pbPacketIn,
      readPacketCallbackWrapper_([pbPacketIn](CmaChannel& channel, TLock lock) {
        channel.onPacket_(*pbPacketIn, lock);
      }));
}

void CmaChannel::onPacket_(const proto::Packet& pbPacketIn, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);

  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kNotification);
  onNotification_(pbPacketIn.notification(), lock);

  // Arm connection to wait for next packet.
  readPacket_();
}

void CmaChannel::onNotification_(
    const proto::Notification& pbNotification,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);

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

void CmaChannel::handleError_(TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);

  // Move pending operations to stack.
  auto sendOperations = std::move(sendOperations_);

  // Release lock before executing callbacks.
  lock.unlock();

  // Notify pending send callbacks of error.
  for (auto& op : sendOperations) {
    op.callback(error_);
  }
}

} // namespace cma
} // namespace channel
} // namespace tensorpipe
