/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

std::shared_ptr<ContextImpl> ContextImpl::create(
    std::shared_ptr<CpuContext> cpuContext) {
  Error error;
  CudaLib cudaLib;
  std::tie(error, cudaLib) = CudaLib::create();
  if (error) {
    TP_VLOG(5)
        << "CUDA basic channel is not viable because libcuda could not be loaded: "
        << error.what();
    return std::make_shared<ContextImpl>();
  }

  if (!cpuContext->isViable()) {
    return std::make_shared<ContextImpl>();
  }

  return std::make_shared<ContextImpl>(
      std::move(cudaLib), std::move(cpuContext));
}

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/false,
          /*domainDescriptor=*/"") {}

ContextImpl::ContextImpl(
    CudaLib cudaLib,
    std::shared_ptr<CpuContext> cpuContext)
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/true,
          cpuContext->domainDescriptor()),
      cudaLib_(std::move(cudaLib)),
      cpuContext_(std::move(cpuContext)) {}

std::shared_ptr<CudaChannel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint endpoint) {
  auto cpuChannel =
      cpuContext_->createChannel(std::move(connections), endpoint);
  return createChannelInternal(std::move(cpuChannel), cudaLoop_);
}

size_t ContextImpl::numConnectionsNeeded() const {
  return cpuContext_->numConnectionsNeeded();
}

const CudaLib& ContextImpl::getCudaLib() {
  return cudaLib_;
}

void ContextImpl::closeImpl() {
  if (cpuContext_ != nullptr) {
    cpuContext_->close();
  }
  cudaLoop_.close();
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
