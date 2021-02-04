/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/channel/cuda_context.h>
#include <tensorpipe/common/cuda_buffer.h>
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

  bool canCommunicateWithRemote(
      const std::string& remoteDomainDescriptor) const;

  const CudaLib& getCudaLib();

  // Implement the DeferredExecutor interface.
  bool inLoop() const override;
  void deferToLoop(std::function<void()> fn) override;

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void closeImpl() override;
  void joinImpl() override;

 private:
  OnDemandDeferredExecutor loop_;

  const CudaLib cudaLib_;
  const NvmlLib nvmlLib_;

  const std::string bootId_;
  const std::vector<std::string> globalUuids_;
  const std::vector<std::vector<bool>> p2pSupport_;
  const std::vector<int> globalIdxOfVisibleDevices_;
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
