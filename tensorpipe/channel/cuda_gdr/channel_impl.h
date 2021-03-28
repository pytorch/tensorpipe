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
#include <tensorpipe/common/state_machine.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

class ContextImpl;

// Ideally we would use NOP_EXTERNAL_STRUCTURE instead of defining the following
// two structs, but we tried so in D26460332 and failed because a bug in GCC 5.5
// (and probably other versions) requires every nop structure used inside a
// std::vector to have an explicit non-defaulted default constructor, which is
// something we cannot do with NOP_EXTERNAL_STRUCTURE and forces us to re-define
// separate structs.

// Replicate the IbvLib::gid struct so we can serialize it with libnop.
struct NopIbvGid {
  uint64_t subnetPrefix;
  uint64_t interfaceId;
  NOP_STRUCTURE(NopIbvGid, subnetPrefix, interfaceId);

  void fromIbvGid(const IbvLib::gid& globalIdentifier) {
    subnetPrefix = globalIdentifier.global.subnet_prefix;
    interfaceId = globalIdentifier.global.interface_id;
  }

  IbvLib::gid toIbvGid() const {
    IbvLib::gid globalIdentifier;
    globalIdentifier.global.subnet_prefix = subnetPrefix;
    globalIdentifier.global.interface_id = interfaceId;
    return globalIdentifier;
  }
};

// Replicate the IbvSetupInformation struct so we can serialize it with libnop.
struct NopIbvSetupInformation {
  // This pointless constructor is needed to work around a bug in GCC 5.5 (and
  // possibly other versions). It appears to be needed in the nop types that
  // are used inside std::vectors.
  NopIbvSetupInformation() {}

  uint32_t localIdentifier;
  NopIbvGid globalIdentifier;
  uint32_t queuePairNumber;
  IbvLib::mtu maximumTransmissionUnit;
  NOP_STRUCTURE(
      NopIbvSetupInformation,
      localIdentifier,
      globalIdentifier,
      queuePairNumber,
      maximumTransmissionUnit);

  void fromIbvSetupInformation(const IbvSetupInformation& setupInfo) {
    localIdentifier = setupInfo.localIdentifier;
    globalIdentifier.fromIbvGid(setupInfo.globalIdentifier);
    queuePairNumber = setupInfo.queuePairNumber;
    maximumTransmissionUnit = setupInfo.maximumTransmissionUnit;
  }

  IbvSetupInformation toIbvSetupInformation() const {
    IbvSetupInformation setupInfo;
    setupInfo.localIdentifier = localIdentifier;
    setupInfo.globalIdentifier = globalIdentifier.toIbvGid();
    setupInfo.queuePairNumber = queuePairNumber;
    setupInfo.maximumTransmissionUnit = maximumTransmissionUnit;
    return setupInfo;
  }
};

struct SendOperation {
  enum State {
    UNINITIALIZED,
    WRITING_DESCRIPTOR,
    READING_READY_TO_RECEIVE,
    WAITING_FOR_CUDA_EVENT,
    SENDING_OVER_IB,
    FINISHED
  };

  // Provide a constructor so we can create the CudaEvent in-place.
  SendOperation(
      CudaBuffer buffer,
      TSendCallback callback,
      size_t localGpuIdx,
      size_t localNicIdx)
      : buffer(buffer),
        callback(std::move(callback)),
        event(localGpuIdx),
        localNicIdx(localNicIdx) {}

  size_t sequenceNumber;
  State state{UNINITIALIZED};
  CudaBuffer buffer;
  TSendCallback callback;
  CudaEvent event;
  size_t localNicIdx;
  ssize_t remoteNicIdx;

  bool doneWritingDescriptor{false};
  bool readReadyToReceive{false};
  bool sendEventReady{false};
  bool sentOverIb{false};
};

struct RecvOperation {
  enum State {
    UNINITIALIZED,
    READING_DESCRIPTOR,
    WAITING_FOR_CUDA_EVENT,
    RECEIVING_OVER_IB_AND_WRITING_READY_TO_RECEIVE,
    FINISHED
  };

  // Provide a constructor so we can create the CudaEvent in-place.
  RecvOperation(
      CudaBuffer buffer,
      TSendCallback callback,
      size_t deviceIdx,
      size_t localNicIdx)
      : buffer(buffer),
        callback(std::move(callback)),
        event(deviceIdx),
        localNicIdx(localNicIdx) {}

  size_t sequenceNumber;
  State state{UNINITIALIZED};
  CudaBuffer buffer;
  TSendCallback callback;
  CudaEvent event;
  size_t localNicIdx;
  size_t remoteNicIdx;

  bool doneReadingDescriptor{false};
  bool recvEventReady{false};
  bool receivedOverIb{false};
};

// First "round" of handshake.
struct HandshakeNumNics {
  size_t numNics;
  NOP_STRUCTURE(HandshakeNumNics, numNics);
};

// Second "round" of handshake.
struct HandshakeSetupInfo {
  std::vector<std::vector<NopIbvSetupInformation>> setupInfo;
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
      TSendCallback callback) override;
  void recvImplFromLoop(
      uint64_t sequenceNumber,
      Buffer buffer,
      TRecvCallback callback) override;
  void handleErrorImpl() override;

 private:
  const std::shared_ptr<transport::Connection> descriptorConnection_;
  const std::shared_ptr<transport::Connection> notificationConnection_;

  enum State {
    INITIALIZING = 1,
    WAITING_FOR_HANDSHAKE_NUM_NICS,
    WAITING_FOR_HANDSHAKE_SETUP_INFO,
    ESTABLISHED,
  };
  State state_{INITIALIZING};

  std::vector<size_t> localGpuToNic_;
  size_t numLocalNics_{0};
  size_t numRemoteNics_{0};

  std::vector<std::vector<IbvQueuePair>> queuePairs_;

  OpsStateMachine<ChannelImpl, SendOperation> sendOps_{
      *this,
      &ChannelImpl::advanceSendOperation};
  using SendOpIter = decltype(sendOps_)::Iter;
  OpsStateMachine<ChannelImpl, RecvOperation> recvOps_{
      *this,
      &ChannelImpl::advanceRecvOperation};
  using RecvOpIter = decltype(recvOps_)::Iter;

  uint32_t numSendsInFlight_{0};
  uint32_t numRecvsInFlight_{0};

  // Callbacks for the initial handshake phase.
  void onReadHandshakeNumNics(const HandshakeNumNics& nopHandshakeNumNics);
  void onReadHandshakeSetupInfo(
      const HandshakeSetupInfo& nopHandshakeSetupInfo);

  // Cleanup methods for teardown.
  void tryCleanup();
  void cleanup();

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
  void writeReadyToSendAndReadReadyToReceive(SendOpIter opIter);
  void waitForSendCudaEvent(SendOpIter opIter);
  void sendOverIb(SendOpIter opIter);
  void callSendCallback(SendOpIter opIter);
  // For recv operations:
  void readDescriptor(RecvOpIter opIter);
  void waitForRecvCudaEvent(RecvOpIter opIter);
  void recvOverIbAndWriteReadyToRecive(RecvOpIter opIter);
  void callRecvCallback(RecvOpIter opIter);

  // Reactions (i.e., callbacks that contribute to complete a state transition).
  // For send operations:
  void onReadReadyToReceive(
      SendOpIter opIter,
      const ReadyToReceive& readyToReceive);
  void onSendEventReady(SendOpIter opIter);
  void onIbvSendDone(SendOpIter opIter);
  // For recv operations:
  void onRecvEventReady(RecvOpIter opIter);
  void onReceivedOverIb(RecvOpIter opIter);
};

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
