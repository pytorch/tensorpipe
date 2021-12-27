/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#include <tensorpipe/common/cpu_buffer.h>
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
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
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
        *nopHolderIn, callbackWrapper_([nopHolderIn](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " done reading nop object (server hello)";
          if (!impl.error_) {
            impl.onClientReadHelloOnConnection(nopHolderIn->getObject());
          }
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
          callbackWrapper_(
              [laneIdx](
                  ChannelImpl& impl,
                  std::shared_ptr<transport::Connection> connection) {
                TP_VLOG(6) << "Channel " << impl.id_
                           << " done requesting connection (for lane "
                           << laneIdx << ")";
                if (!impl.error_) {
                  impl.onServerAcceptOfLane(laneIdx, std::move(connection));
                }
              }));
      laneRegistrationIds_.emplace(laneIdx, token);
      nopLaneAdvertisement.registrationId = token;
      numLanesBeingAccepted_++;
    }
    TP_VLOG(6) << "Channel " << id_ << " writing nop object (server hello)";
    connection_->write(
        *nopHolderOut, callbackWrapper_([nopHolderOut](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " done writing nop object (server hello)";
        }));
  } else {
    TP_THROW_ASSERT() << "unknown endpoint";
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
        callbackWrapper_([laneIdx, nopHolderOut](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " done writing nop object (client hello) on lane "
                     << laneIdx;
        }));
    lanes_[laneIdx] = std::move(lane);
  }

  state_ = ESTABLISHED;
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();
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
    sendOps_.advanceAllOperations();
    recvOps_.advanceAllOperations();
  }
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    size_t length,
    TSendCallback callback) {
  SendOpIter opIter = sendOps_.emplaceBack(sequenceNumber);
  SendOperation& op = *opIter;
  op.ptr = buffer.unwrap<CpuBuffer>().ptr;
  op.length = length;
  op.callback = std::move(callback);

  sendOps_.advanceOperation(opIter);
}

void ChannelImpl::advanceSendOperation(
    SendOpIter opIter,
    SendOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  SendOperation& op = *opIter;

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_ || op.length == 0,
      /*actions=*/{&ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on lanes.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::WRITING_CHUNKS,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >= SendOperation::WRITING_CHUNKS,
      /*actions=*/{&ChannelImpl::writeChunks});

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::WRITING_CHUNKS,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/op.numChunksBeingWritten == 0,
      /*actions=*/{&ChannelImpl::callSendCallback});
}

void ChannelImpl::writeChunks(SendOpIter opIter) {
  SendOperation& op = *opIter;

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
        ptr, length, callbackWrapper_([opIter, laneIdx](ChannelImpl& impl) {
          TP_VLOG(6) << "Channel " << impl.id_ << " done writing payload #"
                     << opIter->sequenceNumber << " on lane " << laneIdx;
          --opIter->numChunksBeingWritten;
          impl.sendOps_.advanceOperation(opIter);
        }));
    ++op.numChunksBeingWritten;
  }
}

void ChannelImpl::callSendCallback(SendOpIter opIter) {
  SendOperation& op = *opIter;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    size_t length,
    TRecvCallback callback) {
  RecvOpIter opIter = recvOps_.emplaceBack(sequenceNumber);
  RecvOperation& op = *opIter;
  op.ptr = buffer.unwrap<CpuBuffer>().ptr;
  op.length = length;
  op.callback = std::move(callback);

  recvOps_.advanceOperation(opIter);
}

void ChannelImpl::advanceRecvOperation(
    RecvOpIter opIter,
    RecvOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  RecvOperation& op = *opIter;

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_ || op.length == 0,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on lanes.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::READING_CHUNKS,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >= RecvOperation::READING_CHUNKS,
      /*actions=*/{&ChannelImpl::readChunks});

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::READING_CHUNKS,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/op.numChunksBeingRead == 0,
      /*actions=*/{&ChannelImpl::callRecvCallback});
}

void ChannelImpl::readChunks(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

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
        callbackWrapper_([opIter, laneIdx](
                             ChannelImpl& impl,
                             const void* /* unused */,
                             size_t /* unused */) {
          TP_VLOG(6) << "Channel " << impl.id_ << " done reading payload #"
                     << opIter->sequenceNumber << " on lane " << laneIdx;
          --opIter->numChunksBeingRead;
          impl.recvOps_.advanceOperation(opIter);
        }));
    ++op.numChunksBeingRead;
  }
}

void ChannelImpl::callRecvCallback(RecvOpIter opIter) {
  RecvOperation& op = *opIter;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
}

void ChannelImpl::handleErrorImpl() {
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();

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
