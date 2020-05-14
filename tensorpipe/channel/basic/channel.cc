/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/channel.h>

#include <algorithm>
#include <list>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/proto/channel/basic.pb.h>

namespace tensorpipe {
namespace channel {
namespace basic {

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

  void initFromLoop_();

  void closeFromLoop_();

  // Arm connection to read next protobuf packet.
  void readPacket_();

  // Called when a protobuf packet was received.
  void onPacket_(const proto::Packet& packet);

  // Called when protobuf packet is a request.
  void onRequest_(const proto::Request& request);

  // Called when protobuf packet is a reply.
  void onReply_(const proto::Reply& reply);

  // Called if send operation was successful.
  void sendCompleted(const uint64_t);

  // Called if recv operation was successful.
  void recvCompleted(const uint64_t);

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
    const void* ptr;
    size_t length;
    TSendCallback callback;
  };

  // State capturing a single recv operation.
  struct RecvOperation {
    const uint64_t id;
    void* ptr;
    size_t length;
    TRecvCallback callback;
  };

  std::list<SendOperation> sendOperations_;
  std::list<RecvOperation> recvOperations_;

  // An identifier for the channel, composed of the identifier for the context,
  // combined with an increasing sequence number. It will only be used for
  // logging and debugging purposes.
  std::string id_;

  // Helpers to prepare callbacks from transports
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

// Send memory region to peer.
void Channel::Impl::sendFromLoop_(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_DCHECK(loop_.inLoop());

  const uint64_t id = nextTensorBeingSent_++;
  TP_VLOG() << "Channel " << id_ << " received a send request (#" << id << ")";

  descriptorCallback =
      [this, id, descriptorCallback{std::move(descriptorCallback)}](
          const Error& error, TDescriptor descriptor) {
        // There is no requirement for the channel to invoke callbacks in order.
        TP_VLOG() << "Channel " << id_ << " is calling a descriptor callback (#"
                  << id << ")";
        descriptorCallback(error, std::move(descriptor));
        TP_VLOG() << "Channel " << id_
                  << " done calling a descriptor callback (#" << id << ")";
      };

  callback = [this, id, callback{std::move(callback)}](const Error& error) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG() << "Channel " << id_ << " is calling a send callback (#" << id
              << ")";
    callback(error);
    TP_VLOG() << "Channel " << id_ << " done calling a send callback (#" << id
              << ")";
  };

  if (error_) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  proto::Descriptor pbDescriptor;

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

  proto::Descriptor pbDescriptor;
  loadDescriptor(pbDescriptor, descriptor);
  const uint64_t id = pbDescriptor.operation_id();
  TP_VLOG() << "Channel " << id_ << " received a recv request (#" << id << ")";

  callback = [this, id, callback{std::move(callback)}](const Error& error) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG() << "Channel " << id_ << " is calling a recv callback (#" << id
              << ")";
    callback(error);
    TP_VLOG() << "Channel " << id_ << " done calling a recv callback (#" << id
              << ")";
  };

  if (error_) {
    callback(error_);
    return;
  }

  recvOperations_.emplace_back(
      RecvOperation{id, ptr, length, std::move(callback)});

  // Ask peer to start sending data now that we have a target pointer.
  auto packet = std::make_shared<proto::Packet>();
  proto::Request* pbRequest = packet->mutable_request();
  pbRequest->set_operation_id(id);
  TP_VLOG() << "Channel " << id_ << " is writing proto (request #" << id << ")";
  connection_->write(*packet, lazyCallbackWrapper_([packet, id](Impl& impl) {
    TP_VLOG() << "Channel " << impl.id_ << " done writing proto (reply #" << id
              << ")";
  }));
  return;
}

void Channel::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
  readPacket_();
}

void Channel::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Channel::Impl::setId(std::string id) {
  TP_VLOG() << "Channel " << id_ << " was renamed to " << id;
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
  TP_VLOG() << "Channel " << id_ << " is closing";
  setError_(TP_CREATE_ERROR(ChannelClosedError));
}

void Channel::Impl::readPacket_() {
  TP_DCHECK(loop_.inLoop());
  auto packet = std::make_shared<proto::Packet>();
  TP_VLOG() << "Channel " << id_ << " is reading proto (request or reply)";
  connection_->read(*packet, lazyCallbackWrapper_([packet](Impl& impl) {
    TP_VLOG() << "Channel " << impl.id_
              << " done reading proto (request or reply)";
    impl.onPacket_(*packet);
  }));
}

void Channel::Impl::onPacket_(const proto::Packet& packet) {
  TP_DCHECK(loop_.inLoop());
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
  TP_DCHECK(loop_.inLoop());
  // Find the send operation matching the request's operation ID.
  const auto id = request.operation_id();
  TP_VLOG() << "Channel " << id_ << " got request (#" << id << ")";
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
  TP_VLOG() << "Channel " << id_ << " is writing proto (reply #" << id << ")";
  connection_->write(
      *pbPacketOut, lazyCallbackWrapper_([pbPacketOut, id](Impl& impl) {
        TP_VLOG() << "Channel " << impl.id_ << " done writing proto (reply #"
                  << id << ")";
      }));

  // Write payload.
  TP_VLOG() << "Channel " << id_ << " is writing payload (#" << id << ")";
  connection_->write(op.ptr, op.length, eagerCallbackWrapper_([id](Impl& impl) {
                       TP_VLOG() << "Channel " << impl.id_
                                 << " done writing payload (#" << id << ")";
                       impl.sendCompleted(id);
                     }));
}

void Channel::Impl::onReply_(const proto::Reply& reply) {
  TP_DCHECK(loop_.inLoop());
  // Find the recv operation matching the reply's operation ID.
  const auto id = reply.operation_id();
  TP_VLOG() << "Channel " << id_ << " got reply (#" << id << ")";
  auto it = std::find_if(
      recvOperations_.begin(), recvOperations_.end(), [id](const auto& op) {
        return op.id == id;
      });
  TP_THROW_ASSERT_IF(it == recvOperations_.end())
      << "Expected recv operation with ID " << id << " to exist.";

  // Reference to operation.
  auto& op = *it;

  // Read payload into specified memory region.
  TP_VLOG() << "Channel " << id_ << " is reading payload (#" << id << ")";
  connection_->read(
      op.ptr,
      op.length,
      eagerCallbackWrapper_(
          [id](Impl& impl, const void* /* unused */, size_t /* unused */) {
            TP_VLOG() << "Channel " << impl.id_ << " done reading payload (#"
                      << id << ")";
            impl.recvCompleted(id);
          }));
}

void Channel::Impl::sendCompleted(const uint64_t id) {
  TP_DCHECK(loop_.inLoop());
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
  TP_DCHECK(loop_.inLoop());
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
  TP_VLOG() << "Channel " << id_ << " is handling error " << error_.what();

  // Close the connection so that all current operations will be aborted. This
  // will cause their callbacks to be invoked, and only then we'll invoke ours.
  connection_->close();
}

} // namespace basic
} // namespace channel
} // namespace tensorpipe
