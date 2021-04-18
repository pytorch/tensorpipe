/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/device.h>

namespace tensorpipe {
namespace channel {
namespace cuda_xth {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create();

  ContextImpl(
      CudaLib cudaLib,
      std::unordered_map<Device, std::string> deviceDescriptors);

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  size_t numConnectionsNeeded() const override;

  const CudaLib& getCudaLib();

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
};

} // namespace cuda_xth
} // namespace channel
} // namespace tensorpipe
