/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/mpt/channel.h>

#include <algorithm>
#include <sstream>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/proto/channel/mpt.pb.h>
#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

namespace {

// State capturing a single send operation.
struct SendOperation {
  uint64_t sequenceNumber;
  const void* ptr;
  size_t length;
  int64_t numChunksBeingWritten{0};
  Channel::TSendCallback callback;
};

// State capturing a single recv operation.
struct RecvOperation {
  uint64_t sequenceNumber;
  void* ptr;
  size_t length;
  int64_t numChunksBeingRead{0};
  Channel::TRecvCallback callback;
};

} // namespace

class Channel::Impl : public std::enable_shared_from_this<Channel::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::shared_ptr<transport::Connection> connection,
      Endpoint endpoint,
      uint64_t numLanes,
      std::string id);

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
  enum State {
    UNINITIALIZED,
    CLIENT_READING_HELLO,
    SERVER_ACCEPTING_LANES,
    ESTABLISHED,
  };

  void initFromLoop_();

  void sendFromLoop_(
      const void* ptr,
      size_t length,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  void recvFromLoop_(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback);

  void setIdFromLoop_(std::string id);

  void closeFromLoop_();

  // Called when client reads the server's hello on backbone connection
  void onClientReadHelloOnConnection_(const proto::Packet& pbPacketIn);

  // Called when server accepts new client connection for lane
  void onServerAcceptOfLane_(
      uint64_t laneIdx,
      std::shared_ptr<transport::Connection> connection);

  // Called when channel endpoint has all lanes established, and processes
  // operations that were performed in the meantime and queued.
  void startSendingAndReceivingUponEstablishingChannel_();

  // Performs the chunking and the writing of one send operation.
  void sendOperation_(SendOperation& op);

  // Performs the chunking and the reading of one recv operation.
  void recvOperation_(RecvOperation& op);

  // Called when the write of one chunk of a send operation has been completed.
  void onWriteOfPayload_(SendOperation& op);

  // Called when the read of one chunk of a recv operation has been completed.
  void onReadOfPayload_(RecvOperation& op);

  void setError_(Error error);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError_();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<transport::Connection> connection_;
  Endpoint endpoint_;
  State state_{UNINITIALIZED};
  uint64_t numLanes_;
  uint64_t numLanesBeingAccepted_{0};
  std::vector<std::shared_ptr<transport::Connection>> lanes_;
  std::unordered_map<uint64_t, uint64_t> laneRegistrationIds_;

  // Increasing identifier for send operations.
  uint64_t nextTensorBeingSent_{0};

  // Increasing identifier for recv operations.
  uint64_t nextTensorBeingReceived_{0};

  std::deque<SendOperation> sendOperations_;
  std::deque<RecvOperation> recvOperations_;

  // An identifier for the channel, composed of the identifier for the context,
  // combined with an increasing sequence number. It will only be used for
  // logging and debugging purposes.
  std::string id_;

  OnDemandLoop loop_;
  Error error_{Error::kSuccess};
  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, this->loop_};
  EagerCallbackWrapper<Impl> eagerCallbackWrapper_{*this, this->loop_};
  ClosingReceiver closingReceiver_;

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
    Endpoint endpoint,
    uint64_t numLanes,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(connection),
          endpoint,
          numLanes,
          std::move(id))) {
  impl_->init();
}

Channel::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection,
    Endpoint endpoint,
    uint64_t numLanes,
    std::string id)
    : context_(std::move(context)),
      connection_(std::move(connection)),
      endpoint_(endpoint),
      numLanes_(numLanes),
      lanes_(numLanes_),
      id_(std::move(id)),
      closingReceiver_(context_, context_->getClosingEmitter()) {}

void Channel::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);

  TP_DCHECK_EQ(state_, UNINITIALIZED);
  if (endpoint_ == Endpoint::kConnect) {
    state_ = CLIENT_READING_HELLO;
    auto packet = std::make_shared<proto::Packet>();
    TP_VLOG(6) << "Channel " << id_ << " reading proto (server hello)";
    connection_->read(*packet, lazyCallbackWrapper_([packet](Impl& impl) {
      TP_VLOG(6) << "Channel " << impl.id_
                 << " done reading proto (server hello)";
      impl.onClientReadHelloOnConnection_(*packet);
    }));
  } else if (endpoint_ == Endpoint::kListen) {
    state_ = SERVER_ACCEPTING_LANES;
    const std::vector<std::string>& addresses = context_->addresses();
    TP_DCHECK_EQ(addresses.size(), numLanes_);
    auto packet = std::make_shared<proto::Packet>();
    proto::ServerHello* pbServerHello = packet->mutable_server_hello();
    for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
      proto::LaneAdvertisement* pbLaneAdvertisement =
          pbServerHello->add_lane_advertisements();
      pbLaneAdvertisement->set_address(addresses[laneIdx]);
      TP_VLOG(6) << "Channel " << id_ << " requesting connection (for lane "
                 << laneIdx << ")";
      uint64_t token = context_->registerConnectionRequest(
          laneIdx,
          lazyCallbackWrapper_(
              [laneIdx](
                  Impl& impl,
                  std::shared_ptr<transport::Connection> connection) {
                TP_VLOG(6) << "Channel " << impl.id_
                           << " done requesting connection (for lane "
                           << laneIdx << ")";
                impl.onServerAcceptOfLane_(laneIdx, std::move(connection));
              }));
      laneRegistrationIds_.emplace(laneIdx, token);
      pbLaneAdvertisement->set_registration_id(token);
      numLanesBeingAccepted_++;
    }
    TP_VLOG(6) << "Channel " << id_ << " writing proto (server hello)";
    connection_->write(*packet, lazyCallbackWrapper_([packet](Impl& impl) {
      TP_VLOG(6) << "Channel " << impl.id_
                 << " done writing proto (server hello)";
    }));
  } else {
    TP_THROW_ASSERT() << "unknown endpoint";
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

  sendOperations_.emplace_back();
  SendOperation& op = sendOperations_.back();
  op.sequenceNumber = sequenceNumber;
  op.ptr = ptr;
  op.length = length;
  op.callback = std::move(callback);

  if (state_ == ESTABLISHED) {
    sendOperation_(op);
  }

  descriptorCallback(Error::kSuccess, std::string());
}

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

  TP_DCHECK_EQ(descriptor, std::string());

  recvOperations_.emplace_back();
  RecvOperation& op = recvOperations_.back();
  op.sequenceNumber = sequenceNumber;
  op.ptr = ptr;
  op.length = length;
  op.callback = std::move(callback);

  if (state_ == ESTABLISHED) {
    recvOperation_(op);
  }
}

void Channel::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Channel::Impl::setId(std::string id) {
  loop_.deferToLoop(
      [this, id{std::move(id)}]() mutable { setIdFromLoop_(std::move(id)); });
}

void Channel::Impl::setIdFromLoop_(std::string id) {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(4) << "Channel " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

void Channel::Impl::onClientReadHelloOnConnection_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, CLIENT_READING_HELLO);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kServerHello);

  const proto::ServerHello& pbServerHello = pbPacketIn.server_hello();
  TP_DCHECK_EQ(pbServerHello.lane_advertisements().size(), numLanes_);
  lanes_.resize(numLanes_);
  for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
    const proto::LaneAdvertisement& pbLaneAdvertisement =
        pbServerHello.lane_advertisements().Get(laneIdx);
    std::shared_ptr<transport::Connection> lane =
        context_->connect(laneIdx, pbLaneAdvertisement.address());
    auto pbPacketOut = std::make_shared<proto::Packet>();
    proto::ClientHello* pbClientHello = pbPacketOut->mutable_client_hello();
    pbClientHello->set_registration_id(pbLaneAdvertisement.registration_id());
    TP_VLOG(6) << "Channel " << id_ << " writing proto (client hello) on lane "
               << laneIdx;
    lane->write(
        *pbPacketOut, lazyCallbackWrapper_([laneIdx, pbPacketOut](Impl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " done writing proto (client hello) on lane "
                     << laneIdx;
        }));
    lanes_[laneIdx] = std::move(lane);
  }

  state_ = ESTABLISHED;
  startSendingAndReceivingUponEstablishingChannel_();
}

void Channel::Impl::onServerAcceptOfLane_(
    uint64_t laneIdx,
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, SERVER_ACCEPTING_LANES);

  TP_DCHECK(!lanes_[laneIdx]);
  TP_DCHECK_LT(laneIdx, lanes_.size());
  lanes_[laneIdx] = std::move(connection);
  auto laneRegistrationIter = laneRegistrationIds_.find(laneIdx);
  TP_DCHECK(laneRegistrationIter != laneRegistrationIds_.end());
  context_->unregisterConnectionRequest(laneRegistrationIter->second);
  laneRegistrationIds_.erase(laneRegistrationIter);
  numLanesBeingAccepted_--;

  if (numLanesBeingAccepted_ == 0) {
    state_ = ESTABLISHED;
    startSendingAndReceivingUponEstablishingChannel_();
  }
}

void Channel::Impl::startSendingAndReceivingUponEstablishingChannel_() {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  for (SendOperation& op : sendOperations_) {
    sendOperation_(op);
  }
  for (RecvOperation& op : recvOperations_) {
    recvOperation_(op);
  }
}

void Channel::Impl::sendOperation_(SendOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  for (uint64_t laneIdx = 0; laneIdx < lanes_.size(); laneIdx++) {
    // Insert "cutpoints" at equally-spaced intervals in the buffer, rounding
    // them down if they don't end up being at an integer position.
    uint64_t offsetStart = op.length * laneIdx / lanes_.size();
    uint64_t offsetEnd = op.length * (laneIdx + 1) / lanes_.size();
    // As void "has no size" we cannot do pointer arithmetic on it. We need to
    // temporarily convert the pointer to a type that has a size of 1 byte.
    const void* ptr = reinterpret_cast<const uint8_t*>(op.ptr) + offsetStart;
    uint64_t length = offsetEnd - offsetStart;

    // Write payload.
    TP_VLOG(6) << "Channel " << id_ << " writing payload #" << op.sequenceNumber
               << " on lane " << laneIdx;
    lanes_[laneIdx]->write(
        ptr, length, eagerCallbackWrapper_([&op, laneIdx](Impl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_ << " done writing payload #"
                     << op.sequenceNumber << " on lane " << laneIdx;
          impl.onWriteOfPayload_(op);
        }));
    ++op.numChunksBeingWritten;
  }
}

void Channel::Impl::recvOperation_(RecvOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  for (uint64_t laneIdx = 0; laneIdx < lanes_.size(); laneIdx++) {
    // Insert "cutpoints" at equally-spaced intervals in the buffer, rounding
    // them down if they don't end up being at an integer position.
    uint64_t offsetStart = op.length * laneIdx / lanes_.size();
    uint64_t offsetEnd = op.length * (laneIdx + 1) / lanes_.size();
    // As void "has no size" we cannot do pointer arithmetic on it. We need to
    // temporarily convert the pointer to a type that has a size of 1 byte.
    void* ptr = reinterpret_cast<uint8_t*>(op.ptr) + offsetStart;
    uint64_t length = offsetEnd - offsetStart;

    // Read payload.
    TP_VLOG(6) << "Channel " << id_ << " reading payload #" << op.sequenceNumber
               << " on lane " << laneIdx;
    lanes_[laneIdx]->read(
        ptr,
        length,
        eagerCallbackWrapper_(
            [&op, laneIdx](
                Impl& impl, const void* /* unused */, size_t /* unused */) {
              TP_VLOG(6) << "Channel " << impl.id_ << " done reading payload #"
                         << op.sequenceNumber << " on lane " << laneIdx;
              impl.onReadOfPayload_(op);
            }));
    ++op.numChunksBeingRead;
  }
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
  TP_VLOG(4) << "Channel " << id_ << " is closing";
  setError_(TP_CREATE_ERROR(ChannelClosedError));
}

void Channel::Impl::onWriteOfPayload_(SendOperation& op) {
  TP_DCHECK(loop_.inLoop());

  --op.numChunksBeingWritten;
  if (op.numChunksBeingWritten > 0) {
    return;
  }

  op.callback(error_);

  TP_DCHECK(!sendOperations_.empty());
  TP_DCHECK(&op == &sendOperations_.front());
  sendOperations_.pop_front();
}

void Channel::Impl::onReadOfPayload_(RecvOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  --op.numChunksBeingRead;
  if (op.numChunksBeingRead > 0) {
    return;
  }

  op.callback(error_);

  TP_DCHECK(!recvOperations_.empty());
  TP_DCHECK(&op == &recvOperations_.front());
  recvOperations_.pop_front();
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
  TP_VLOG(5) << "Channel " << id_ << " is handling error " << error_.what();

  if (state_ != ESTABLISHED) {
    for (SendOperation& op : sendOperations_) {
      TP_DCHECK_EQ(op.numChunksBeingWritten, 0);
      op.callback(error_);
    }
    sendOperations_.clear();
    for (RecvOperation& op : recvOperations_) {
      TP_DCHECK_EQ(op.numChunksBeingRead, 0);
      op.callback(error_);
    }
    recvOperations_.clear();
  }

  // Close the connections so that all current operations will be aborted. This
  // will cause their callbacks to be invoked, and only then we'll invoke ours.
  connection_->close();
  for (auto& lane : lanes_) {
    if (lane) {
      lane->close();
    }
  }

  for (const auto& iter : laneRegistrationIds_) {
    context_->unregisterConnectionRequest(iter.second);
  }
}

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
