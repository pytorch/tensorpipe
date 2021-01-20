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
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

namespace {

std::string deviceUUID(int device) {
  cudaDeviceProp props;
  TP_CUDA_CHECK(cudaGetDeviceProperties(&props, device));

  std::ostringstream uuid;
  uuid << std::hex;

#if CUDART_VERSION > 9020
  uuid << std::setfill('0') << std::setw(2);
  for (int j = 0; j < 16; ++j) {
    // The bitmask is required otherwise a negative value will get promoted to
    // (signed) int with sign extension if char is signed.
    uuid << (props.uuid.bytes[j] & 0xff);
  }
#else // CUDART_VERSION <= 9020
  // CUDA <=9.2 does not have a uuid device property. Falling back to using PCI
  // ids.
  uuid << props.pciDomainID << "," << props.pciBusID << ","
       << props.pciDeviceID;
#endif

  return uuid.str();
}

std::string generateDomainDescriptor() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  oss << bootID.value();

  int deviceCount;
  TP_CUDA_CHECK(cudaGetDeviceCount(&deviceCount));
  std::vector<std::string> uuids(deviceCount);
  for (int i = 0; i < deviceCount; ++i) {
    uuids[i] = deviceUUID(i);
  }

  std::sort(uuids.begin(), uuids.end());
  for (const auto& uuid : uuids) {
    oss << "-" << uuid;
  }

  return oss.str();
}

} // namespace

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          generateDomainDescriptor()) {
  Error error;
  std::tie(error, cudaLib_) = CudaLib::create();
  if (error) {
    TP_VLOG(5) << "Channel context " << id_
               << " is not viable because libcuda could not be loaded: "
               << error.what();
    return;
  }
  foundCudaLib_ = true;
}

std::shared_ptr<CudaChannel> ContextImpl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint /* unused */) {
  return createChannelInternal(std::move(connection));
}

bool ContextImpl::isViable() const {
  if (!foundCudaLib_) {
    return false;
  }

  // This part is largely inspired from
  // https://github.com/NVIDIA/cuda-samples/blob/master/Samples/simpleIPC/simpleIPC.cu.
  int deviceCount;
  TP_CUDA_CHECK(cudaGetDeviceCount(&deviceCount));
  for (int i = 0; i < deviceCount; ++i) {
    cudaDeviceProp props;
    TP_CUDA_CHECK(cudaGetDeviceProperties(&props, i));

    // Unified addressing is required for IPC.
    if (!props.unifiedAddressing) {
      TP_VLOG(4) << "Channel context " << id_
                 << " is not viable because CUDA device " << i
                 << " does not have unified addressing";
      return false;
    }

    // The other two compute modes are "exclusive" and "prohibited", both of
    // which prevent access from an other process.
    if (props.computeMode != cudaComputeModeDefault) {
      TP_VLOG(4) << "Channel context " << id_
                 << " is not viable because CUDA device " << i
                 << " is not in default compute mode";
      return false;
    }

    for (int j = 0; j < deviceCount; ++j) {
      // cudaDeviceCanAccessPeer() returns false when the two devices are the
      // same.
      if (i == j) {
        continue;
      }

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

const CudaLib& ContextImpl::getCudaLib() {
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
