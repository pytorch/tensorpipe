/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_xth/context_impl.h>

#include <unistd.h>

#include <functional>
#include <sstream>
#include <string>
#include <utility>

#include <tensorpipe/channel/cuda_xth/channel_impl.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cuda_xth {

std::shared_ptr<ContextImpl> ContextImpl::create() {
  Error error;
  CudaLib cudaLib;
  std::tie(error, cudaLib) = CudaLib::create();
  if (error) {
    TP_VLOG(5)
        << "CUDA XTH channel is not viable because libcuda could not be loaded: "
        << error.what();
    return nullptr;
  }

  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  auto nsID = getLinuxNamespaceId(LinuxNamespace::kPid);
  if (!nsID) {
    TP_VLOG(5)
        << "CUDA XTH channel is not viable because it couldn't determine the PID namespace ID";
    return nullptr;
  }
  oss << bootID.value() << "_" << nsID.value() << "_" << ::getpid();
  const std::string domainDescriptor = oss.str();

  std::unordered_map<Device, std::string> deviceDescriptors;
  for (const auto& device : getCudaDevices(cudaLib)) {
    cudaDeviceProp props;
    TP_CUDA_CHECK(cudaGetDeviceProperties(&props, device.index));

    // Unified addressing is required for cross-device `cudaMemcpyAsync()`. We
    // could lift this requirement by adding a fallback to
    // `cudaMemcpyPeerAsync()`.
    if (!props.unifiedAddressing) {
      TP_VLOG(4) << "CUDA XTH channel is not viable because CUDA device "
                 << device.index << " does not have unified addressing";
      return nullptr;
    }
    deviceDescriptors[device] = domainDescriptor;
  }

  if (deviceDescriptors.empty()) {
    return nullptr;
  }

  return std::make_shared<ContextImpl>(
      std::move(cudaLib), std::move(deviceDescriptors));
}

ContextImpl::ContextImpl(
    CudaLib cudaLib,
    std::unordered_map<Device, std::string> deviceDescriptors)
    : ContextImplBoilerplate<ContextImpl, ChannelImpl>(
          std::move(deviceDescriptors)),
      cudaLib_(std::move(cudaLib)) {}

std::shared_ptr<Channel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(
      std::move(connections[0]), std::move(connections[1]));
}

size_t ContextImpl::numConnectionsNeeded() const {
  return 2;
}

const CudaLib& ContextImpl::getCudaLib() {
  return cudaLib_;
}

void ContextImpl::handleErrorImpl() {}

void ContextImpl::joinImpl() {}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

} // namespace cuda_xth
} // namespace channel
} // namespace tensorpipe
