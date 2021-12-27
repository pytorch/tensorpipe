/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_basic/context_impl.h>

#include <functional>
#include <memory>
#include <utility>

#include <tensorpipe/channel/cuda_basic/channel_impl.h>
#include <tensorpipe/channel/cuda_basic/constants.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/nop.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

namespace {

struct DeviceDescriptor {
  std::string deviceType;
  std::string descriptor;
  NOP_STRUCTURE(DeviceDescriptor, deviceType, descriptor);
};

DeviceDescriptor deserializeDeviceDescriptor(
    const std::string& deviceDescriptor) {
  NopHolder<DeviceDescriptor> nopHolder;
  loadDescriptor(nopHolder, deviceDescriptor);
  return std::move(nopHolder.getObject());
}

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create(
    std::shared_ptr<Context> cpuContext) {
  Error error;
  CudaLib cudaLib;
  std::tie(error, cudaLib) = CudaLib::create();
  if (error) {
    TP_VLOG(5)
        << "CUDA basic channel is not viable because libcuda could not be loaded: "
        << error.what();
    return nullptr;
  }

  if (cpuContext->deviceDescriptors().count(Device{kCpuDeviceType, 0}) == 0) {
    TP_THROW_ASSERT() << "CUDA basic channel needs a CPU channel";

    return nullptr;
  }

  if (!cpuContext->isViable()) {
    return nullptr;
  }

  std::unordered_map<Device, std::string> deviceDescriptors;
  // NOTE: Assume there is only one CPU.
  TP_DCHECK_EQ(
      cpuContext->deviceDescriptors().count(Device{kCpuDeviceType, 0}), 1);
  const auto cpuDeviceDescriptor =
      cpuContext->deviceDescriptors().begin()->second;

  NopHolder<DeviceDescriptor> nopHolder;
  DeviceDescriptor& deviceDescriptor = nopHolder.getObject();
  deviceDescriptor.descriptor = cpuDeviceDescriptor;

  deviceDescriptor.deviceType = kCpuDeviceType;
  deviceDescriptors[Device{kCpuDeviceType, 0}] = saveDescriptor(nopHolder);
  for (const auto& device : getCudaDevices(cudaLib)) {
    deviceDescriptor.deviceType = kCudaDeviceType;
    deviceDescriptors[device] = saveDescriptor(nopHolder);
  }

  return std::make_shared<ContextImpl>(
      std::move(cudaLib), std::move(cpuContext), std::move(deviceDescriptors));
}

ContextImpl::ContextImpl(
    CudaLib cudaLib,
    std::shared_ptr<Context> cpuContext,
    std::unordered_map<Device, std::string> deviceDescriptors)
    : ContextImplBoilerplate<ContextImpl, ChannelImpl>(
          std::move(deviceDescriptors)),
      cudaLib_(std::move(cudaLib)),
      cpuContext_(std::move(cpuContext)) {}

std::shared_ptr<Channel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint endpoint) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  auto conn = std::move(connections.back());
  connections.pop_back();
  auto cpuChannel =
      cpuContext_->createChannel(std::move(connections), endpoint);
  return createChannelInternal(
      std::move(conn), std::move(cpuChannel), cudaLoop_);
}

size_t ContextImpl::numConnectionsNeeded() const {
  return 1 + cpuContext_->numConnectionsNeeded();
}

bool ContextImpl::canCommunicateWithRemote(
    const std::string& localDeviceDescriptor,
    const std::string& remoteDeviceDescriptor) const {
  DeviceDescriptor nopLocalDeviceDescriptor =
      deserializeDeviceDescriptor(localDeviceDescriptor);
  DeviceDescriptor nopRemoteDeviceDescriptor =
      deserializeDeviceDescriptor(remoteDeviceDescriptor);

  // Prevent CudaBasic from being mistakenly used for CPU to CPU transfers, as
  // there are always better options.
  if (nopLocalDeviceDescriptor.deviceType == kCpuDeviceType &&
      nopRemoteDeviceDescriptor.deviceType == kCpuDeviceType) {
    return false;
  }

  return nopLocalDeviceDescriptor.descriptor ==
      nopRemoteDeviceDescriptor.descriptor;
}

const CudaLib& ContextImpl::getCudaLib() {
  return cudaLib_;
}

Allocator& ContextImpl::getCudaHostSendAllocator(int deviceIdx) {
  if (!cudaHostSendAllocator_.has_value()) {
    CudaPinnedBuffer buffer = makeCudaPinnedBuffer(kStagingAreaSize, deviceIdx);
    uint8_t* ptr = buffer.get();
    cudaHostSendAllocator_.emplace(CudaHostAllocator{
        std::move(buffer), Allocator(ptr, kNumSlots, kSlotSize)});
  }

  return cudaHostSendAllocator_->allocator;
}

Allocator& ContextImpl::getCudaHostRecvAllocator(int deviceIdx) {
  if (!cudaHostRecvAllocator_.has_value()) {
    CudaPinnedBuffer buffer = makeCudaPinnedBuffer(kStagingAreaSize, deviceIdx);
    uint8_t* ptr = buffer.get();
    cudaHostRecvAllocator_.emplace(CudaHostAllocator{
        std::move(buffer), Allocator(ptr, kNumSlots, kSlotSize)});
  }

  return cudaHostRecvAllocator_->allocator;
}

void ContextImpl::handleErrorImpl() {
  if (cpuContext_ != nullptr) {
    cpuContext_->close();
  }
  cudaLoop_.close();

  if (cudaHostSendAllocator_.has_value()) {
    cudaHostSendAllocator_->allocator.close();
  }
  if (cudaHostRecvAllocator_.has_value()) {
    cudaHostRecvAllocator_->allocator.close();
  }
}

void ContextImpl::joinImpl() {
  if (cpuContext_ != nullptr) {
    cpuContext_->join();
  }
  cudaLoop_.join();
}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

void ContextImpl::setIdImpl() {
  cpuContext_->setId(id_ + ".cpu");
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
