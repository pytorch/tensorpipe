/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <tensorpipe/channel/cuda_context.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/cuda_event_pool.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/nvml_lib.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create();

  ContextImpl();

  ContextImpl(
      std::string domainDescriptor,
      CudaLib cudaLib,
      NvmlLib nvmlLib,
      std::string bootId,
      std::vector<std::string> globalUuids,
      std::vector<std::vector<bool>> p2pSupport,
      std::vector<int> globalIdxOfVisibleDevices);

  std::shared_ptr<CudaChannel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  size_t numConnectionsNeeded() const override;

  bool canCommunicateWithRemote(
      const std::string& remoteDomainDescriptor) const override;

  const CudaLib& getCudaLib();

  const std::string& getProcessIdentifier();

  void* openIpcHandle(
      std::string allocationId,
      const cudaIpcMemHandle_t& remoteHandle,
      int deviceIdx);

  // Creating CUDA IPC events "on-the-fly" risks causing a deadlock, due to a
  // bug in the CUDA driver that was supposedly fixed in version 460. However,
  // to support earlier versions, we create a pool of events at the beginning
  // and re-use them for all transfers.
  void requestSendEvent(int deviceIdx, CudaEventPool::RequestCallback callback);
  void requestRecvEvent(int deviceIdx, CudaEventPool::RequestCallback callback);

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

  const std::string bootId_;
  const std::vector<std::string> globalUuids_;
  const std::vector<std::vector<bool>> p2pSupport_;
  const std::vector<int> globalIdxOfVisibleDevices_;

  // A combination of the process's PID namespace and its PID, which combined
  // with CUDA's buffer ID should allow us to uniquely identify each allocation
  // on the current machine.
  std::string processIdentifier_;

  // Map from device pointer and device index, to the number of times that
  // handle has been opened. Needed to keep track of what cudaIpcCloseMemHandle
  // calls we need to make at closing.
  // FIXME Turn this into an unordered_map.
  // FIXME It may make sense to break the string into the process identifier
  // which indexes a nested map which is keyed by the buffer ID (+ the device
  // index), because each channel will only ever look up handles for the same
  // process identifier, hence we could do that first loopup once and cache it.
  std::map<std::tuple<std::string, int>, void*> openIpcHandles_;

  std::vector<std::unique_ptr<CudaEventPool>> sendEventPools_;
  std::vector<std::unique_ptr<CudaEventPool>> recvEventPools_;
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
