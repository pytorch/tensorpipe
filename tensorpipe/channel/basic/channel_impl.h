/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>

#include <tensorpipe/channel/channel_impl_boilerplate.h>
#include <tensorpipe/common/state_machine.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace basic {

class ContextImpl;

struct SendOperation {
  enum State { UNINITIALIZED, WRITING, FINISHED };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneWriting{false};

  // Arguments at creation
  const void* ptr;
  size_t length;
  TSendCallback callback;
};

// State capturing a single recv operation.
struct RecvOperation {
  enum State { UNINITIALIZED, READING, FINISHED };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneReading{false};

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
      std::shared_ptr<transport::Connection> connection);

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
  const std::shared_ptr<transport::Connection> connection_;

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
  void write(SendOpIter opIter);
  void callSendCallback(SendOpIter opIter);
  // For recv operations:
  void read(RecvOpIter opIter);
  void callRecvCallback(RecvOpIter opIter);
};

} // namespace basic
} // namespace channel
} // namespace tensorpipe
