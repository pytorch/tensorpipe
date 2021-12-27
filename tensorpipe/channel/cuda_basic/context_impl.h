/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/common/allocator.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/cuda_loop.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create(
      std::shared_ptr<Context> cpuContext);

  ContextImpl(
      CudaLib cudaLib,
      std::shared_ptr<Context> cpuContext,
      std::unordered_map<Device, std::string> deviceDescriptors);

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  size_t numConnectionsNeeded() const override;

  bool canCommunicateWithRemote(
      const std::string& localDeviceDescriptor,
      const std::string& remoteDeviceDescriptor) const override;

  const CudaLib& getCudaLib();
  Allocator& getCudaHostSendAllocator(int deviceIdx);
  Allocator& getCudaHostRecvAllocator(int deviceIdx);

  // Implement the DeferredExecutor interface.
  bool inLoop() const override;
  void deferToLoop(std::function<void()> fn) override;

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void handleErrorImpl() override;
  void joinImpl() override;
  void setIdImpl() override;

 private:
  OnDemandDeferredExecutor loop_;

  const CudaLib cudaLib_;

  const std::shared_ptr<Context> cpuContext_;
  // TODO: Lazy initialization of cuda loop.
  CudaLoop cudaLoop_;

  struct CudaHostAllocator {
    CudaPinnedBuffer buffer;
    Allocator allocator;
  };
  optional<CudaHostAllocator> cudaHostSendAllocator_;
  optional<CudaHostAllocator> cudaHostRecvAllocator_;
};

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
