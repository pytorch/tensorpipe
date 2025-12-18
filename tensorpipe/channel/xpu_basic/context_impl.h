/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/common/allocator.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/xpu.h>
#include <tensorpipe/common/xpu_buffer.h>
#include <tensorpipe/common/xpu_loop.h>

namespace tensorpipe {
namespace channel {
namespace xpu_basic {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create(
      std::shared_ptr<Context> cpuContext);

  ContextImpl(
      std::shared_ptr<Context> cpuContext,
      std::unordered_map<Device, std::string> deviceDescriptors);

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  size_t numConnectionsNeeded() const override;

  bool canCommunicateWithRemote(
      const std::string& localDeviceDescriptor,
      const std::string& remoteDeviceDescriptor) const override;

  Allocator& getXpuHostSendAllocator(sycl::queue&);
  Allocator& getXpuHostRecvAllocator(sycl::queue&);

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

  const std::shared_ptr<Context> cpuContext_;
  // TODO: Lazy initialization of xpu loop.
  XpuLoop xpuLoop_;

  struct XpuHostAllocator {
    xpu::XpuPinnedBuffer buffer;
    Allocator allocator;
  };
  optional<XpuHostAllocator> xpuHostSendAllocator_;
  optional<XpuHostAllocator> xpuHostRecvAllocator_;
};

} // namespace xpu_basic
} // namespace channel
} // namespace tensorpipe
