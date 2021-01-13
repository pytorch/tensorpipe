/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_ipc/context_impl.h>

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include <tensorpipe/channel/cuda_ipc/channel_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

namespace {

std::string generateDomainDescriptor() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  oss << bootID.value();
  return oss.str();
}

} // namespace

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          generateDomainDescriptor()) {
  Error error;
  std::tie(error, cudaLib_) = CudaLib::create();
  if (!error) {
    foundCudaLib_ = true;
  }
}

std::shared_ptr<CudaChannel> ContextImpl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint /* unused */) {
  return createChannelInternal(std::move(connection));
}

bool ContextImpl::isViable() const {
  if (!foundCudaLib_) {
    TP_VLOG(4) << "Channel context " << id_
               << " is not viable because libcuda could not be loaded";
    return false;
  }

  int deviceCount;
  TP_CUDA_CHECK(cudaGetDeviceCount(&deviceCount));
  for (int i = 0; i < deviceCount; ++i) {
    cudaDeviceProp props;
    TP_CUDA_CHECK(cudaGetDeviceProperties(&props, i));

    if (!props.unifiedAddressing) {
      TP_VLOG(4) << "Channel context " << id_
                 << " is not viable because CUDA device " << i
                 << " does not have unified addressing";
      return false;
    }

    if (!props.computeMode != cudaComputeModeDefault) {
      TP_VLOG(4) << "Channel context " << id_
                 << " is not viable because CUDA device " << i
                 << " is not in default compute mode";
      return false;
    }

    for (int j = 0; j < deviceCount; ++j) {
      int canAccessPeer;
      TP_CUDA_CHECK(cudaDeviceCanAccessPeer(&canAccessPeer, i, j));
      if (!canAccessPeer) {
        TP_VLOG(4) << "Channel context " << id_
                   << " is not viable because CUDA device " << i
                   << " cannot access peer device " << j;
        return false;
      }
    }
  }

  return true;
}

CudaLib& ContextImpl::getCudaLib() {
  return cudaLib_;
}

void ContextImpl::closeImpl() {}

void ContextImpl::joinImpl() {}

bool ContextImpl::inLoop() {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
