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
#include <utility>
#include <vector>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/channel_impl_boilerplate.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/ibv.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

NOP_EXTERNAL_STRUCTURE(IbvLib::gid, raw);
NOP_EXTERNAL_STRUCTURE(
    IbvSetupInformation,
    localIdentifier,
    globalIdentifier,
    queuePairNumber,
    maximumTransmissionUnit);

namespace channel {
namespace cuda_gdr {

class ContextImpl;

struct SendOperation {
  // Provide a constructor so we can create the CudaEvent in-place.
  SendOperation(
      size_t sequenceNumber,
      CudaBuffer buffer,
      TRecvCallback callback,
      size_t localGpuIdx,
      size_t localNicIdx)
      : sequenceNumber(sequenceNumber),
        buffer(buffer),
        callback(std::move(callback)),
        event(localGpuIdx),
        localNicIdx(localNicIdx) {}

  size_t sequenceNumber;
  CudaBuffer buffer;
  TSendCallback callback;
  CudaEvent event;
  size_t localNicIdx;
  size_t remoteNicIdx;
};

struct RecvOperation {
  // Provide a constructor so we can create the CudaEvent in-place.
  RecvOperation(
      size_t sequenceNumber,
      CudaBuffer buffer,
      TSendCallback callback,
      size_t deviceIdx,
      size_t localNicIdx,
      size_t remoteNicIdx)
      : sequenceNumber(sequenceNumber),
        buffer(buffer),
        callback(std::move(callback)),
        event(deviceIdx),
        localNicIdx(localNicIdx),
        remoteNicIdx(remoteNicIdx) {}

  size_t sequenceNumber;
  CudaBuffer buffer;
  TSendCallback callback;
  CudaEvent event;
  size_t localNicIdx;
  size_t remoteNicIdx;
};

// First "round" of handshake.
struct HandshakeNumNics {
  size_t numNics;
  NOP_STRUCTURE(HandshakeNumNics, numNics);
};

// Second "round" of handshake.
struct HandshakeSetupInfo {
  std::vector<std::vector<IbvSetupInformation>> setupInfo;
  NOP_STRUCTURE(HandshakeSetupInfo, setupInfo);
};

// From sender to receiver (through pipe).
struct Descriptor {
  size_t originNicIdx;
  NOP_STRUCTURE(Descriptor, originNicIdx);
};

// From receiver to sender (through channel's connection).
struct ReadyToReceive {
  size_t destinationNicIdx;
  NOP_STRUCTURE(ReadyToReceive, destinationNicIdx);
};

class ChannelImpl final
    : public ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl> {
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
      CudaBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback) override;
  void recvImplFromLoop(
      uint64_t sequenceNumber,
      TDescriptor descriptor,
      CudaBuffer buffer,
      TRecvCallback callback) override;
  void handleErrorImpl() override;

 private:
  const std::shared_ptr<transport::Connection> connection_;

  enum State {
    INITIALIZING = 1,
    WAITING_FOR_HANDSHAKE_NUM_NICS,
    WAITING_FOR_HANDSHAKE_SETUP_INFO,
    ESTABLISHED,
  };
  State state_{INITIALIZING};

  void onReadHandshakeNumNics(const HandshakeNumNics& nopHandshakeNumNics);
  void onReadHandshakeSetupInfo(
      const HandshakeSetupInfo& nopHandshakeSetupInfo);

  std::vector<size_t> localGpuToNic_;
  size_t numLocalNics_{0};
  size_t numRemoteNics_{0};

  std::vector<std::vector<IbvQueuePair>> queuePairs_;

  std::list<SendOperation> sendOps_;
  std::list<RecvOperation> recvOps_;

  uint32_t numSendsInFlight_{0};
  uint32_t numRecvsInFlight_{0};

  void processSendOperationFromLoop(SendOperation& op);
  void onReadReadyToReceive(
      SendOperation& op,
      const ReadyToReceive& readyToReceive);
  void onSendEventReady(SendOperation& op);
  void onIbvSendDone(SendOperation& op);
  void eraseOp(const SendOperation& op);

  void processRecvOperationFromLoop(RecvOperation& op);
  void onRecvEventReady(RecvOperation& op);
  void onIbvRecvDone(RecvOperation& op);
  void eraseOp(const RecvOperation& op);

  void tryCleanup();
  void cleanup();
};

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
