/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/intrap/channel.h>

#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <limits>
#include <list>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/common/system.h>
#include <tensorpipe/proto/channel/intrap.pb.h>

namespace tensorpipe {
namespace channel {
namespace intrap {

class Channel::Impl : public std::enable_shared_from_this<Channel::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface>,
      std::shared_ptr<transport::Connection>,
      std::string);

  // Called by the channel's constructor.
  void init();

  void send(
      const void* ptr,
      size_t length,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  void recv(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback);

  // Tell the channel what its identifier is.
  void setId(std::string id);

  void close();

 private:
  OnDemandLoop loop_;

  void initFromLoop_();

  // Send memory region to peer.
  void sendFromLoop_(
      const void* ptr,
      size_t length,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  // Receive memory region from peer.
  void recvFromLoop_(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback);

  void closeFromLoop_();

  // Arm connection to read next protobuf packet.
  void readPacket_();

  // Called when a protobuf packet was received.
  void onPacket_(const proto::Packet& packet);

  // Called when protobuf packet is a notification.
  void onNotification_(const proto::Notification& notification);

  void setError_(Error error);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError_();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<transport::Connection> connection_;
  Error error_{Error::kSuccess};

  ClosingReceiver closingReceiver_;

  // Increasing identifier for send operations.
  uint64_t nextTensorBeingSent_{0};

  // State capturing a single send operation.
  struct SendOperation {
    const uint64_t id;
    TSendCallback callback;
  };

  std::list<SendOperation> sendOperations_;

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
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
  readPacket_();
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
  loop_.deferToLoop([this,
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
  TP_DCHECK(loop_.inLoop());

  if (error_) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  proto::Descriptor pbDescriptor;

  const auto id = nextTensorBeingSent_++;
  pbDescriptor.set_operation_id(id);
  pbDescriptor.set_ptr(reinterpret_cast<std::uintptr_t>(ptr));

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
  loop_.deferToLoop([this,
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
  TP_DCHECK(loop_.inLoop());

  if (error_) {
    callback(error_);
    return;
  }

  proto::Descriptor pbDescriptor;
  loadDescriptor(pbDescriptor, descriptor);
  const uint64_t id = pbDescriptor.operation_id();
  void* remotePtr = reinterpret_cast<void*>(pbDescriptor.ptr());

  context_->requestCopy(
      remotePtr,
      ptr,
      length,
      eagerCallbackWrapper_([id, callback{std::move(callback)}](Impl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done copying payload #" << id;
        // Let peer know we've completed the copy.
        auto pbPacketOut = std::make_shared<proto::Packet>();
        proto::Notification* pbNotification =
            pbPacketOut->mutable_notification();
        pbNotification->set_operation_id(id);
        TP_VLOG(6) << "Channel " << impl.id_
                   << " is writing proto (notification #" << id << ")";
        impl.connection_->write(
            *pbPacketOut,
            impl.lazyCallbackWrapper_([pbPacketOut, id](Impl& impl) {
              TP_VLOG(6) << "Channel " << impl.id_
                         << " done writing proto (notification #" << id << ")";
            }));
        callback(impl.error_);
      }));
}

void Channel::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Channel::Impl::setId(std::string id) {
  TP_VLOG(4) << "Channel " << id_ << " was renamed to " << id;
  // FIXME Should we defer this to the loop?
  id_ = std::move(id);
}

void Channel::close() {
  impl_->close();
}

Channel::~Channel() {
  close();
}

void Channel::Impl::close() {
  loop_.deferToLoop([this]() { closeFromLoop_(); });
}

void Channel::Impl::closeFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  setError_(TP_CREATE_ERROR(ChannelClosedError));
}

void Channel::Impl::readPacket_() {
  TP_DCHECK(loop_.inLoop());
  auto pbPacketIn = std::make_shared<proto::Packet>();
  connection_->read(*pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
    impl.onPacket_(*pbPacketIn);
  }));
}

void Channel::Impl::onPacket_(const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());

  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kNotification);
  onNotification_(pbPacketIn.notification());

  // Arm connection to wait for next packet.
  readPacket_();
}

void Channel::Impl::onNotification_(const proto::Notification& pbNotification) {
  TP_DCHECK(loop_.inLoop());

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

void Channel::Impl::setError_(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError_();
}

void Channel::Impl::handleError_() {
  TP_DCHECK(loop_.inLoop());

  // Move pending operations to stack.
  auto sendOperations = std::move(sendOperations_);

  // Notify pending send callbacks of error.
  for (auto& op : sendOperations) {
    op.callback(error_);
  }

  connection_->close();
}

} // namespace intrap
} // namespace channel
} // namespace tensorpipe
