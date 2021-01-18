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

ContextImpl::ContextImpl(std::shared_ptr<CpuContext> cpuContext)
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          cpuContext->domainDescriptor()),
      cpuContext_(std::move(cpuContext)) {
  Error error;
  std::tie(error, cudaLibHandle_) = loadCuda();
  if (error) {
    TP_VLOG(5) << "Channel context " << id_
               << " is not viable because libcuda could not be loaded: "
               << error.what();
    return;
  }
}

std::shared_ptr<CudaChannel> ContextImpl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint endpoint) {
  auto cpuChannel = cpuContext_->createChannel(std::move(connection), endpoint);
  return createChannelInternal(std::move(cpuChannel), cudaLoop_);
}

bool ContextImpl::isViable() const {
  return (cudaLibHandle_ != nullptr);
}

void ContextImpl::closeImpl() {
  cpuContext_->close();
  cudaLoop_.close();
}

void ContextImpl::joinImpl() {
  cpuContext_->join();
  cudaLoop_.join();
}

bool ContextImpl::inLoop() {
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
