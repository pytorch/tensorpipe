/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/mpt/channel_impl.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <tensorpipe/channel/mpt/context_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection,
    Endpoint endpoint,
    uint64_t numLanes)
    : ChannelImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)),
      endpoint_(endpoint),
      numLanes_(numLanes),
      lanes_(numLanes_) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);

  TP_DCHECK_EQ(state_, UNINITIALIZED);
  if (endpoint_ == Endpoint::kConnect) {
    state_ = CLIENT_READING_HELLO;
    auto nopHolderIn = std::make_shared<NopHolder<Packet>>();
    TP_VLOG(6) << "Channel " << id_ << " reading nop object (server hello)";
    connection_->read(
        *nopHolderIn, lazyCallbackWrapper_([nopHolderIn](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " done reading nop object (server hello)";
          impl.onClientReadHelloOnConnection(nopHolderIn->getObject());
        }));
  } else if (endpoint_ == Endpoint::kListen) {
    state_ = SERVER_ACCEPTING_LANES;
    const std::vector<std::string>& addresses = context_->addresses();
    TP_DCHECK_EQ(addresses.size(), numLanes_);
    auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
    Packet& nopPacket = nopHolderOut->getObject();
    nopPacket.Become(nopPacket.index_of<ServerHello>());
    ServerHello& nopServerHello = *nopPacket.get<ServerHello>();
    for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
      nopServerHello.laneAdvertisements.emplace_back();
      LaneAdvertisement& nopLaneAdvertisement =
          nopServerHello.laneAdvertisements.back();
      nopLaneAdvertisement.address = addresses[laneIdx];
      TP_VLOG(6) << "Channel " << id_ << " requesting connection (for lane "
                 << laneIdx << ")";
      uint64_t token = context_->registerConnectionRequest(
          laneIdx,
          lazyCallbackWrapper_(
              [laneIdx](
                  ChannelImpl& impl,
                  std::shared_ptr<transport::Connection> connection) {
                TP_VLOG(6) << "Channel " << impl.id_
                           << " done requesting connection (for lane "
                           << laneIdx << ")";
                impl.onServerAcceptOfLane(laneIdx, std::move(connection));
              }));
      laneRegistrationIds_.emplace(laneIdx, token);
      nopLaneAdvertisement.registrationId = token;
      numLanesBeingAccepted_++;
    }
    TP_VLOG(6) << "Channel " << id_ << " writing nop object (server hello)";
    connection_->write(
        *nopHolderOut, lazyCallbackWrapper_([nopHolderOut](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " done writing nop object (server hello)";
        }));
  } else {
    TP_THROW_ASSERT() << "unknown endpoint";
  }
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CpuBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  sendOperations_.emplace_back();
  SendOperation& op = sendOperations_.back();
  op.sequenceNumber = sequenceNumber;
  op.ptr = buffer.ptr;
  op.length = buffer.length;
  op.callback = std::move(callback);

  if (state_ == ESTABLISHED) {
    sendOperation(op);
  }

  descriptorCallback(Error::kSuccess, std::string());
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CpuBuffer buffer,
    TRecvCallback callback) {
  TP_DCHECK_EQ(descriptor, std::string());

  recvOperations_.emplace_back();
  RecvOperation& op = recvOperations_.back();
  op.sequenceNumber = sequenceNumber;
  op.ptr = buffer.ptr;
  op.length = buffer.length;
  op.callback = std::move(callback);

  if (state_ == ESTABLISHED) {
    recvOperation(op);
  }
}

void ChannelImpl::onClientReadHelloOnConnection(const Packet& nopPacketIn) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, CLIENT_READING_HELLO);
  TP_DCHECK_EQ(nopPacketIn.index(), nopPacketIn.index_of<ServerHello>());

  const ServerHello& nopServerHello = *nopPacketIn.get<ServerHello>();
  TP_DCHECK_EQ(nopServerHello.laneAdvertisements.size(), numLanes_);
  lanes_.resize(numLanes_);
  for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
    const LaneAdvertisement& nopLaneAdvertisement =
        nopServerHello.laneAdvertisements[laneIdx];
    std::shared_ptr<transport::Connection> lane =
        context_->connect(laneIdx, nopLaneAdvertisement.address);
    auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
    Packet& nopPacket = nopHolderOut->getObject();
    nopPacket.Become(nopPacket.index_of<ClientHello>());
    ClientHello& nopClientHello = *nopPacket.get<ClientHello>();
    nopClientHello.registrationId = nopLaneAdvertisement.registrationId;
    TP_VLOG(6) << "Channel " << id_
               << " writing nop object (client hello) on lane " << laneIdx;
    lane->write(
        *nopHolderOut,
        lazyCallbackWrapper_([laneIdx, nopHolderOut](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " done writing nop object (client hello) on lane "
                     << laneIdx;
        }));
    lanes_[laneIdx] = std::move(lane);
  }

  state_ = ESTABLISHED;
  startSendingAndReceivingUponEstablishingChannel();
}

void ChannelImpl::onServerAcceptOfLane(
    uint64_t laneIdx,
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(context_->inLoop());
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
    startSendingAndReceivingUponEstablishingChannel();
  }
}

void ChannelImpl::startSendingAndReceivingUponEstablishingChannel() {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  for (SendOperation& op : sendOperations_) {
    sendOperation(op);
  }
  for (RecvOperation& op : recvOperations_) {
    recvOperation(op);
  }
}

void ChannelImpl::sendOperation(SendOperation& op) {
  TP_DCHECK(context_->inLoop());
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
        ptr, length, eagerCallbackWrapper_([&op, laneIdx](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_ << " done writing payload #"
                     << op.sequenceNumber << " on lane " << laneIdx;
          impl.onWriteOfPayload(op);
        }));
    ++op.numChunksBeingWritten;
  }
}

void ChannelImpl::recvOperation(RecvOperation& op) {
  TP_DCHECK(context_->inLoop());
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
        eagerCallbackWrapper_([&op, laneIdx](
                                  ChannelImpl& impl,
                                  const void* /* unused */,
                                  size_t /* unused */) {
          TP_VLOG(6) << "Channel " << impl.id_ << " done reading payload #"
                     << op.sequenceNumber << " on lane " << laneIdx;
          impl.onReadOfPayload(op);
        }));
    ++op.numChunksBeingRead;
  }
}

void ChannelImpl::onWriteOfPayload(SendOperation& op) {
  TP_DCHECK(context_->inLoop());

  --op.numChunksBeingWritten;
  if (op.numChunksBeingWritten > 0) {
    return;
  }

  op.callback(error_);

  TP_DCHECK(!sendOperations_.empty());
  TP_DCHECK(&op == &sendOperations_.front());
  sendOperations_.pop_front();
}

void ChannelImpl::onReadOfPayload(RecvOperation& op) {
  TP_DCHECK(context_->inLoop());
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

void ChannelImpl::handleErrorImpl() {
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

  context_->unenroll(*this);
}

// TODO Implement setIdImpl to propagate the ID to the connections

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
