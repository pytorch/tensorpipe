/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

namespace {

std::string generateDomainDescriptor() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";

  pid_t pid = getpid();

  // Combine boot ID and PID.
  oss << bootID.value() << "-" << pid;

  return oss.str();
}

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create() {
  Error error;
  CudaLib cudaLib;
  std::tie(error, cudaLib) = CudaLib::create();
  if (error) {
    TP_VLOG(5)
        << "CUDA XTH channel is not viable because libcuda could not be loaded: "
        << error.what();
    return std::make_shared<ContextImpl>();
  }

  int deviceCount;
  TP_CUDA_CHECK(cudaGetDeviceCount(&deviceCount));
  for (int i = 0; i < deviceCount; ++i) {
    cudaDeviceProp props;
    TP_CUDA_CHECK(cudaGetDeviceProperties(&props, i));

    // Unified addressing is required for cross-device `cudaMemcpyAsync()`. We
    // could lift this requirement by adding a fallback to
    // `cudaMemcpyPeerAsync()`.
    if (!props.unifiedAddressing) {
      TP_VLOG(4) << "CUDA XTH channel is not viable because CUDA device " << i
                 << " does not have unified addressing";
      return std::make_shared<ContextImpl>();
    }
  }

  return std::make_shared<ContextImpl>(std::move(cudaLib));
}

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/false,
          /*domainDescriptor=*/"") {}

ContextImpl::ContextImpl(CudaLib cudaLib)
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/true,
          generateDomainDescriptor()),
      cudaLib_(std::move(cudaLib)) {}

std::shared_ptr<CudaChannel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(std::move(connections[0]));
}

const CudaLib& ContextImpl::getCudaLib() {
  return cudaLib_;
}

void ContextImpl::closeImpl() {}

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
