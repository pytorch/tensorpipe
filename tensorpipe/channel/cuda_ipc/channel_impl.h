/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <list>
#include <memory>
#include <string>

#include <cuda_runtime.h>

#include <tensorpipe/channel/channel_impl_boilerplate.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_event_pool.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/state_machine.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

class ContextImpl;

struct ChunkSendOperation {
  enum State { UNINITIALIZED, REQUESTING_EVENT, READING_REPLY, FINISHED };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneRequestingEvent{false};
  bool doneReadingReply{false};

  // Arguments at creation
  const uint64_t bufferSequenceNumber;
  const size_t chunkId;
  const size_t numChunks;
  const void* const ptr;
  const size_t length;
  const int deviceIdx;
  const cudaStream_t stream;
  TSendCallback callback;

  // Other data
  CudaEventPool::BorrowedEvent startEv;
  std::string stopEvHandle;

  ChunkSendOperation(
      uint64_t bufferSequenceNumber,
      size_t chunkId,
      size_t numChunks,
      TSendCallback callback,
      int deviceIdx,
      const void* ptr,
      size_t length,
      cudaStream_t stream);
};

struct ChunkRecvOperation {
  enum State {
    UNINITIALIZED,
    READING_DESCRIPTOR,
    REQUESTING_EVENT,
    READING_ACK,
    FINISHED
  };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneReadingDescriptor{false};
  bool doneRequestingEvent{false};
  bool doneReadingAck{false};

  // Arguments at creation
  const uint64_t bufferSequenceNumber;
  const size_t chunkId;
  const size_t numChunks;
  void* const ptr;
  const size_t length;
  const int deviceIdx;
  const cudaStream_t stream;
  TRecvCallback callback;

  // Other data
  CudaEventPool::BorrowedEvent stopEv;
  std::string allocationId;
  std::string bufferHandle;
  size_t offset;
  std::string startEvHandle;

  ChunkRecvOperation(
      uint64_t bufferSequenceNumber,
      size_t chunkId,
      size_t numChunks,
      TRecvCallback callback,
      int deviceIdx,
      void* ptr,
      size_t length,
      cudaStream_t stream);
};

class ChannelImpl final
    : public ChannelImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  ChannelImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::shared_ptr<transport::Connection> descriptorConnection,
      std::shared_ptr<transport::Connection> replyConnection,
      std::shared_ptr<transport::Connection> ackConnection);

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
  const std::shared_ptr<transport::Connection> replyConnection_;
  const std::shared_ptr<transport::Connection> ackConnection_;

  // A sequence number for the chunks.
  uint64_t nextChunkBeingSent_{0};
  uint64_t nextChunkBeingReceived_{0};

  OpsStateMachine<ChannelImpl, ChunkSendOperation> chunkSendOps_{
      *this,
      &ChannelImpl::advanceChunkSendOperation};
  using ChunkSendOpIter = decltype(chunkSendOps_)::Iter;
  OpsStateMachine<ChannelImpl, ChunkRecvOperation> chunkRecvOps_{
      *this,
      &ChannelImpl::advanceChunkRecvOperation};
  using ChunkRecvOpIter = decltype(chunkRecvOps_)::Iter;

  // State machines for send and recv ops.
  void advanceChunkSendOperation(
      ChunkSendOpIter opIter,
      ChunkSendOperation::State prevOpState);
  void advanceChunkRecvOperation(
      ChunkRecvOpIter opIter,
      ChunkRecvOperation::State prevOpState);

  // Actions (i.e., methods that begin a state transition).
  // For send operations:
  void requestEvent(ChunkSendOpIter opIter);
  void recordStartEvent(ChunkSendOpIter opIter);
  void writeDescriptor(ChunkSendOpIter opIter);
  void readReply(ChunkSendOpIter opIter);
  void returnEvent(ChunkSendOpIter opIter);
  void waitOnStopEvent(ChunkSendOpIter opIter);
  void callSendCallback(ChunkSendOpIter opIter);
  void writeAck(ChunkSendOpIter opIter);
  // For recv operations:
  void readDescriptor(ChunkRecvOpIter opIter);
  void requestEvent(ChunkRecvOpIter opIter);
  void waitOnStartEventAndCopyAndRecordStopEvent(ChunkRecvOpIter opIter);
  void callRecvCallback(ChunkRecvOpIter opIter);
  void writeReply(ChunkRecvOpIter opIter);
  void readAck(ChunkRecvOpIter opIter);
  void returnEvent(ChunkRecvOpIter opIter);
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
