/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/common/allocator.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/common/nvml_lib.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create();

  ContextImpl(
      std::unordered_map<Device, std::string> deviceDescriptors,
      CudaLib cudaLib,
      NvmlLib nvmlLib,
      std::vector<std::string> globalUuids,
      std::vector<std::vector<bool>> p2pSupport,
      std::string processIdentifier);

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  size_t numConnectionsNeeded() const override;

  bool canCommunicateWithRemote(
      const std::string& localDeviceDescriptor,
      const std::string& remoteDeviceDescriptor) const override;

  const CudaLib& getCudaLib();

  // Takes the index of the slot, the (smart) pointer to the slot, and the (raw)
  // pointer to the event for the slot.
  using SlotAllocCallback =
      std::function<void(const Error&, size_t, Allocator::TChunk, CudaEvent*)>;
  void allocateSlot(int deviceIdx, size_t length, SlotAllocCallback callback);

  struct OutboxInfo {
    std::string processIdentifier;
    std::string memHandle;
    std::vector<std::string> eventHandles;
  };
  OutboxInfo getLocalOutboxInfo(int deviceIdx);

  struct RemoteOutboxHandle {
    CudaIpcBuffer buffer;
    std::unique_ptr<optional<CudaEvent>[]> events;
  };
  const RemoteOutboxHandle& openRemoteOutbox(
      int localDeviceIdx,
      int remoteDeviceIdx,
      OutboxInfo remoteOutboxInfo);

  // Implement the DeferredExecutor interface.
  bool inLoop() const override;
  void deferToLoop(std::function<void()> fn) override;

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void handleErrorImpl() override;
  void joinImpl() override;

 private:
  OnDemandDeferredExecutor loop_;

  const CudaLib cudaLib_;
  const NvmlLib nvmlLib_;

  const std::vector<std::string> globalUuids_;
  const std::vector<std::vector<bool>> p2pSupport_;

  // A combination of the process's PID namespace and its PID, which combined
  // with the device index allows us to uniquely identify each staging buffer on
  // the current machine.
  const std::string processIdentifier_;

  // A CUDA on-device allocation that acts as the outbox for all the channels of
  // this context. We cannot directly get and open IPC handles of the user's
  // buffers, as this will fail if the user already opened such a handle (this
  // limitation was lifted in CUDA 11.1). Moreover, since we "leak" the opened
  // IPC handles (i.e., we leave them open, and close them all when the context
  // closes), if we opened an IPC handle to a user buffer and the user freed
  // that buffer we would prevent CUDA from really making that memory available
  // again (this is an undocumented behavior which was observed experimentally).
  // As a solution, we create our own allocation and get and open an IPC handle
  // to that, as we can guarantee its lifetime and that no other IPC handle
  // exists. We then use it as a staging ground for outgoing transfers, copying
  // chunks to it from source buffers, and having the remote copy them to the
  // target buffer.
  struct Outbox {
    const CudaDeviceBuffer buffer;
    std::unique_ptr<optional<CudaEvent>[]> events;
    const cudaIpcMemHandle_t handle;
    const std::vector<cudaIpcEventHandle_t> eventHandles;
    Allocator allocator;

    explicit Outbox(int deviceIdx);
    ~Outbox();
  };
  std::vector<std::unique_ptr<Outbox>> outboxes_;

  struct RemoteOutboxKey {
    std::string processIdentifier;
    int remoteDeviceIdx;
    int localDeviceIdx;

    bool operator==(const RemoteOutboxKey& other) const noexcept {
      return processIdentifier == other.processIdentifier &&
          remoteDeviceIdx == other.remoteDeviceIdx &&
          localDeviceIdx == other.localDeviceIdx;
    }
  };
  struct RemoteOutboxKeyHash {
    size_t operator()(const RemoteOutboxKey& key) const noexcept {
      size_t h1 = std::hash<std::string>{}(key.processIdentifier);
      size_t h2 = std::hash<int>{}(key.remoteDeviceIdx);
      size_t h3 = std::hash<int>{}(key.localDeviceIdx);
      // Byte-shift hashes in order to "capture" the order of members.
      // FIXME Should we use a proper hash combiner? We can copy Boost's one.
      return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
  };
  std::unordered_map<RemoteOutboxKey, RemoteOutboxHandle, RemoteOutboxKeyHash>
      remoteOutboxes_;
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
