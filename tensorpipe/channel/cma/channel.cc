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
#include <list>
#include <mutex>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cma/context_impl.h>
#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/common/system.h>
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

class Channel::Impl : public std::enable_shared_from_this<Channel::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::shared_ptr<transport::Connection> connection,
      std::string id);

  // Called by the channel's constructor.
  void init();

  void send(
      CpuBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  void recv(TDescriptor descriptor, CpuBuffer buffer, TRecvCallback callback);

  // Tell the channel what its identifier is.
  void setId(std::string id);

  void close();

 private:
  OnDemandDeferredExecutor loop_;

  void initFromLoop();

  // Send memory region to peer.
  void sendFromLoop(
      CpuBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  // Receive memory region from peer.
  void recvFromLoop(
      TDescriptor descriptor,
      CpuBuffer buffer,
      TRecvCallback callback);

  void setIdFromLoop(std::string id);

  void closeFromLoop();

  void setError(Error error);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<transport::Connection> connection_;
  Error error_{Error::kSuccess};

  ClosingReceiver closingReceiver_;

  // Increasing identifier for send operations.
  uint64_t nextTensorBeingSent_{0};

  // Increasing identifier for recv operations.
  uint64_t nextTensorBeingReceived_{0};

  // An identifier for the channel, composed of the identifier for the context,
  // combined with an increasing sequence number. It will only be used for
  // logging and debugging purposes.
  std::string id_;

  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, this->loop_};
  EagerCallbackWrapper<Impl> eagerCallbackWrapper_{*this, this->loop_};

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::LazyCallbackWrapper;
  template <typename T>
  friend class tensorpipe::EagerCallbackWrapper;
};

Channel::Channel(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(connection),
          std::move(id))) {
  impl_->init();
}

Channel::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection,
    std::string id)
    : context_(std::move(context)),
      connection_(std::move(connection)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

void Channel::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop(); });
}

void Channel::Impl::initFromLoop() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
}

void Channel::send(
    CpuBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  impl_->send(buffer, std::move(descriptorCallback), std::move(callback));
}

void Channel::Impl::send(
    CpuBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  loop_.deferToLoop([this,
                     buffer,
                     descriptorCallback{std::move(descriptorCallback)},
                     callback{std::move(callback)}]() mutable {
    sendFromLoop(buffer, std::move(descriptorCallback), std::move(callback));
  });
}

void Channel::Impl::sendFromLoop(
    CpuBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_DCHECK(loop_.inLoop());

  const uint64_t sequenceNumber = nextTensorBeingSent_++;
  TP_VLOG(4) << "Channel " << id_ << " received a send request (#"
             << sequenceNumber << ")";

  descriptorCallback = [this,
                        sequenceNumber,
                        descriptorCallback{std::move(descriptorCallback)}](
                           const Error& error, TDescriptor descriptor) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG(4) << "Channel " << id_ << " is calling a descriptor callback (#"
               << sequenceNumber << ")";
    descriptorCallback(error, std::move(descriptor));
    TP_VLOG(4) << "Channel " << id_ << " done calling a descriptor callback (#"
               << sequenceNumber << ")";
  };

  callback = [this, sequenceNumber, callback{std::move(callback)}](
                 const Error& error) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG(4) << "Channel " << id_ << " is calling a send callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a send callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  TP_VLOG(6) << "Channel " << id_ << " is reading notification (#"
             << sequenceNumber << ")";
  connection_->read(
      nullptr,
      0,
      eagerCallbackWrapper_(
          [sequenceNumber, callback{std::move(callback)}](
              Impl& impl, const void* /* unused */, size_t /* unused */) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done reading notification (#" << sequenceNumber
                       << ")";
            callback(impl.error_);
          }));

  NopHolder<Descriptor> nopHolder;
  Descriptor& nopDescriptor = nopHolder.getObject();
  nopDescriptor.pid = getpid();
  nopDescriptor.ptr = reinterpret_cast<uint64_t>(buffer.ptr);

  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
}

// Receive memory region from peer.
void Channel::recv(
    TDescriptor descriptor,
    CpuBuffer buffer,
    TRecvCallback callback) {
  impl_->recv(std::move(descriptor), buffer, std::move(callback));
}

void Channel::Impl::recv(
    TDescriptor descriptor,
    CpuBuffer buffer,
    TRecvCallback callback) {
  loop_.deferToLoop([this,
                     descriptor{std::move(descriptor)},
                     buffer,
                     callback{std::move(callback)}]() mutable {
    recvFromLoop(std::move(descriptor), buffer, std::move(callback));
  });
}

void Channel::Impl::recvFromLoop(
    TDescriptor descriptor,
    CpuBuffer buffer,
    TRecvCallback callback) {
  TP_DCHECK(loop_.inLoop());

  const uint64_t sequenceNumber = nextTensorBeingReceived_++;
  TP_VLOG(4) << "Channel " << id_ << " received a recv request (#"
             << sequenceNumber << ")";

  callback = [this, sequenceNumber, callback{std::move(callback)}](
                 const Error& error) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG(4) << "Channel " << id_ << " is calling a recv callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a recv callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    callback(error_);
    return;
  }

  NopHolder<Descriptor> nopHolder;
  loadDescriptor(nopHolder, descriptor);
  Descriptor& nopDescriptor = nopHolder.getObject();
  pid_t remotePid = nopDescriptor.pid;
  void* remotePtr = reinterpret_cast<void*>(nopDescriptor.ptr);

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#" << sequenceNumber
             << ")";
  context_->requestCopy(
      remotePid,
      remotePtr,
      buffer.ptr,
      buffer.length,
      eagerCallbackWrapper_([sequenceNumber,
                             callback{std::move(callback)}](Impl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done copying payload (#"
                   << sequenceNumber << ")";

        // Let peer know we've completed the copy.
        TP_VLOG(6) << "Channel " << impl.id_ << " is writing notification (#"
                   << sequenceNumber << ")";
        impl.connection_->write(
            nullptr, 0, impl.lazyCallbackWrapper_([sequenceNumber](Impl& impl) {
              TP_VLOG(6) << "Channel " << impl.id_
                         << " done writing notification (#" << sequenceNumber
                         << ")";
            }));

        callback(impl.error_);
      }));
}

void Channel::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Channel::Impl::setId(std::string id) {
  loop_.deferToLoop(
      [this, id{std::move(id)}]() mutable { setIdFromLoop(std::move(id)); });
}

void Channel::Impl::setIdFromLoop(std::string id) {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(4) << "Channel " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

void Channel::close() {
  impl_->close();
}

Channel::~Channel() {
  close();
}

void Channel::Impl::close() {
  loop_.deferToLoop([this]() { closeFromLoop(); });
}

void Channel::Impl::closeFromLoop() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(4) << "Channel " << id_ << " is closing";
  setError(TP_CREATE_ERROR(ChannelClosedError));
}

void Channel::Impl::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void Channel::Impl::handleError() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(5) << "Channel " << id_ << " is handling error " << error_.what();

  connection_->close();
}

} // namespace cma
} // namespace channel
} // namespace tensorpipe
