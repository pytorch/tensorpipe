/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <tensorpipe/channel/channel_impl_boilerplate.h>
#include <tensorpipe/channel/mpt/nop_types.h>
#include <tensorpipe/common/state_machine.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

class ContextImpl;

// State capturing a single send operation.
struct SendOperation {
  enum State { UNINITIALIZED, WRITING_CHUNKS, FINISHED };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  int64_t numChunksBeingWritten{0};

  // Arguments at creation
  const void* ptr;
  size_t length;
  TSendCallback callback;
};

// State capturing a single recv operation.
struct RecvOperation {
  enum State { UNINITIALIZED, READING_CHUNKS, FINISHED };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  int64_t numChunksBeingRead{0};

  // Arguments at creation
  void* ptr;
  size_t length;
  TRecvCallback callback;
};

class ChannelImpl final
    : public ChannelImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  ChannelImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::shared_ptr<transport::Connection> connection,
      Endpoint endpoint,
      uint64_t numLanes);

 protected:
  // Implement the entry points called by ChannelImplBoilerplate.
  void initImplFromLoop() override;
  void sendImplFromLoop(
      uint64_t sequenceNumber,
      Buffer buffer,
      size_t length,
      TSendCallback callback) override;
  void recvImplFromLoop(
      uint64_t sequenceNumber,
      Buffer buffer,
      size_t length,
      TRecvCallback callback) override;
  void handleErrorImpl() override;

 private:
  enum State {
    UNINITIALIZED,
    CLIENT_READING_HELLO,
    SERVER_ACCEPTING_LANES,
    ESTABLISHED,
  };

  // Called when client reads the server's hello on backbone connection
  void onClientReadHelloOnConnection(const Packet& nopPacketIn);

  // Called when server accepts new client connection for lane
  void onServerAcceptOfLane(
      uint64_t laneIdx,
      std::shared_ptr<transport::Connection> connection);

  const std::shared_ptr<transport::Connection> connection_;
  const Endpoint endpoint_;
  State state_{UNINITIALIZED};
  const uint64_t numLanes_;
  uint64_t numLanesBeingAccepted_{0};
  std::vector<std::shared_ptr<transport::Connection>> lanes_;
  std::unordered_map<uint64_t, uint64_t> laneRegistrationIds_;

  OpsStateMachine<ChannelImpl, SendOperation> sendOps_{
      *this,
      &ChannelImpl::advanceSendOperation};
  using SendOpIter = decltype(sendOps_)::Iter;
  OpsStateMachine<ChannelImpl, RecvOperation> recvOps_{
      *this,
      &ChannelImpl::advanceRecvOperation};
  using RecvOpIter = decltype(recvOps_)::Iter;

  // State machines for send and recv ops.
  void advanceSendOperation(
      SendOpIter opIter,
      SendOperation::State prevOpState);
  void advanceRecvOperation(
      RecvOpIter opIter,
      RecvOperation::State prevOpState);

  // Actions (i.e., methods that begin a state transition).
  // For send operations:
  void writeChunks(SendOpIter opIter);
  void callSendCallback(SendOpIter opIter);
  // For recv operations:
  void readChunks(RecvOpIter opIter);
  void callRecvCallback(RecvOpIter opIter);
};

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
