/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
namespace cma {

class ContextImpl;

struct SendOperation {
  enum State {
    UNINITIALIZED,
    WRITING_DESCRIPTOR,
    READING_NOTIFICATION,
    FINISHED
  };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneWritingDescriptor{false};
  bool doneReadingNotification{false};

  // Arguments at creation
  void* ptr;
  TSendCallback callback;
};

struct RecvOperation {
  enum State { UNINITIALIZED, READING_DESCRIPTOR, COPYING, FINISHED };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneReadingDescriptor{false};
  bool doneCopying{false};

  // Arguments at creation
  void* ptr;
  size_t length;
  TRecvCallback callback;

  // Other data
  pid_t remotePid;
  void* remotePtr;
};

class ChannelImpl final
    : public ChannelImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  ChannelImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::shared_ptr<transport::Connection> descriptorConnection,
      std::shared_ptr<transport::Connection> notificationConnection);

 protected:
  // Implement the entry points called by ChannelImplBoilerplate.
  void initImplFromLoop() override;
  void sendImplFromLoop(
      uint64_t sequenceNumber,
      Buffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback) override;
  void recvImplFromLoop(
      uint64_t sequenceNumber,
      TDescriptor descriptor,
      Buffer buffer,
      TRecvCallback callback) override;
  void handleErrorImpl() override;

 private:
  const std::shared_ptr<transport::Connection> descriptorConnection_;
  const std::shared_ptr<transport::Connection> notificationConnection_;

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
  void writeDescriptor(SendOpIter opIter);
  void readNotification(SendOpIter opIter);
  void callSendCallback(SendOpIter opIter);
  // For recv operations:
  void readDescriptor(RecvOpIter opIter);
  void copy(RecvOpIter opIter);
  void callRecvCallback(RecvOpIter opIter);
  void writeNotification(RecvOpIter opIter);
};

} // namespace cma
} // namespace channel
} // namespace tensorpipe
