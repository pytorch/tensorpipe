/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#include <tensorpipe/channel/cuda_ipc/context_impl.h>
#include <tensorpipe/common/allocator.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/state_machine.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

class ContextImpl;

struct ChunkSendOperation {
  enum State {
    UNINITIALIZED,
    ALLOCATING_STAGING_BUFFER,
    READING_REPLY,
    FINISHED
  };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneAllocatingStagingBuffer{false};
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
  size_t slotIdx{static_cast<size_t>(-1)};
  Allocator::TChunk stagingBuffer;
  CudaEvent* event{nullptr};

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
  enum State { UNINITIALIZED, READING_DESCRIPTOR, FINISHED };

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
  int remoteDeviceIdx;
  size_t remoteSlotIdx;

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
      std::shared_ptr<transport::Connection> replyConnection);

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
  const std::shared_ptr<transport::Connection> descriptorConnection_;
  const std::shared_ptr<transport::Connection> replyConnection_;

  // For each local device, whether we've already sent the information about the
  // device's outbox to the remote, who needs it to open a handle to the outbox.
  // Used during the send path.
  std::vector<bool> localOutboxesSent_;

  // For each remote device, the information about the remote's outbox for that
  // device (or nullopt, if we haven't received it yet). We store it because we
  // will only receive it once (for the first buffer coming from that device)
  // but we might need it multiple time, as we need to open it for every local
  // target device where it might be needed. Used during the receive path.
  std::vector<optional<ContextImpl::OutboxInfo>> remoteOutboxesReceived_;
  // For each remote and local device, the handle to the opened remote outbox
  // for that device (or nullptr if we haven't opened it yet). Used during the
  // receive path.
  std::vector<std::vector<const ContextImpl::RemoteOutboxHandle*>>
      remoteOutboxesOpened_;

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
  void allocateStagingBuffer(ChunkSendOpIter opIter);
  void copyFromSourceToStaging(ChunkSendOpIter opIter);
  void writeDescriptor(ChunkSendOpIter opIter);
  void readReply(ChunkSendOpIter opIter);
  void releaseStagingBuffer(ChunkSendOpIter opIter);
  void callSendCallback(ChunkSendOpIter opIter);
  // For recv operations:
  void readDescriptor(ChunkRecvOpIter opIter);
  void copyFromStagingToTarget(ChunkRecvOpIter opIter);
  void callRecvCallback(ChunkRecvOpIter opIter);
  void writeReply(ChunkRecvOpIter opIter);
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
