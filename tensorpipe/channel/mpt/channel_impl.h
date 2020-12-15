/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

// State capturing a single send operation.
struct SendOperation {
  uint64_t sequenceNumber;
  const void* ptr;
  size_t length;
  int64_t numChunksBeingWritten{0};
  TSendCallback callback;
};

// State capturing a single recv operation.
struct RecvOperation {
  uint64_t sequenceNumber;
  void* ptr;
  size_t length;
  int64_t numChunksBeingRead{0};
  TRecvCallback callback;
};

class ContextImpl;

class ChannelImpl final
    : public ChannelImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl> {
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
      CpuBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback) override;
  void recvImplFromLoop(
      uint64_t sequenceNumber,
      TDescriptor descriptor,
      CpuBuffer buffer,
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

  // Called when channel endpoint has all lanes established, and processes
  // operations that were performed in the meantime and queued.
  void startSendingAndReceivingUponEstablishingChannel();

  // Performs the chunking and the writing of one send operation.
  void sendOperation(SendOperation& op);

  // Performs the chunking and the reading of one recv operation.
  void recvOperation(RecvOperation& op);

  // Called when the write of one chunk of a send operation has been completed.
  void onWriteOfPayload(SendOperation& op);

  // Called when the read of one chunk of a recv operation has been completed.
  void onReadOfPayload(RecvOperation& op);

  const std::shared_ptr<transport::Connection> connection_;
  const Endpoint endpoint_;
  State state_{UNINITIALIZED};
  const uint64_t numLanes_;
  uint64_t numLanesBeingAccepted_{0};
  std::vector<std::shared_ptr<transport::Connection>> lanes_;
  std::unordered_map<uint64_t, uint64_t> laneRegistrationIds_;

  std::deque<SendOperation> sendOperations_;
  std::deque<RecvOperation> recvOperations_;
};

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
