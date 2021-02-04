/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_ipc/context_impl.h>

#include <algorithm>
#include <array>
#include <functional>
#include <iomanip>
#include <ios>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cuda_ipc/channel_impl.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/nop.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

namespace {

// FIXME This is duplicated in CMA, find a place in common for it.
std::string joinStrs(const std::vector<std::string>& strs) {
  if (strs.empty()) {
    return "";
  }
  std::ostringstream oss;
  oss << strs[0];
  for (size_t idx = 1; idx < strs.size(); idx++) {
    oss << ", " << strs[idx];
  }
  return oss.str();
}

template <typename T>
std::string formatMatrix(const std::vector<std::vector<T>>& matrix) {
  std::ostringstream oss;
  oss << "{";
  for (size_t rowIdx = 0; rowIdx < matrix.size(); rowIdx++) {
    if (rowIdx > 0) {
      oss << ", ";
    }
    oss << "{";
    for (size_t colIdx = 0; colIdx < matrix[rowIdx].size(); colIdx++) {
      if (colIdx > 0) {
        oss << ", ";
      }
      oss << matrix[rowIdx][colIdx];
    }
    oss << "}";
  }
  oss << "}";
  return oss.str();
}

// Since text manipulation is hard, let's use this to double-check our results.
bool isValidUuid(const std::string& uuid) {
  // Check it's in this format:
  // aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
  // |0   |5   |10  |15  |20  |25  |30  |35
  if (uuid.size() != 36) {
    return false;
  }
  for (int i = 0; i < uuid.size(); i++) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (uuid[i] != '-') {
        return false;
      }
    } else {
      if (!((uuid[i] >= '0' && uuid[i] <= '9') ||
            (uuid[i] >= 'a' && uuid[i] <= 'f'))) {
        return false;
      }
    }
  }
  return true;
}

std::string getUuidOfCudaDev(const CudaLib& cudaLib, int devIdx) {
  CUdevice device;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.deviceGet(&device, devIdx));

  CUuuid uuid;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.deviceGetUuid(&uuid, device));

  // The CUDA driver and NVML choose two different format for UUIDs, hence we
  // need to reconcile them. We do so using the most human readable format, that
  // is "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" (8-4-4-4-12).
  std::ostringstream uuidSs;
  uuidSs << std::hex << std::setfill('0');
  for (int j = 0; j < 16; ++j) {
    // The bitmask is required otherwise a negative value will get promoted to
    // (signed) int with sign extension if char is signed.
    uuidSs << std::setw(2) << (uuid.bytes[j] & 0xff);
    if (j == 3 || j == 5 || j == 7 || j == 9) {
      uuidSs << '-';
    }
  }

  std::string uuidStr = uuidSs.str();
  TP_THROW_ASSERT_IF(!isValidUuid(uuidStr))
      << "Couldn't obtain valid UUID for GPU #" << devIdx
      << " from CUDA driver. Got: " << uuidStr;
  return uuidStr;
}

std::tuple<std::vector<std::string>, std::vector<std::vector<bool>>>
getGlobalUuidsAndP2pSupport(const NvmlLib& nvmlLib) {
  unsigned int numDevices;
  TP_NVML_CHECK(nvmlLib, nvmlLib.deviceGetCount_v2(&numDevices));

  std::vector<nvmlDevice_t> devices(numDevices);
  std::vector<std::string> uuids(numDevices);
  for (unsigned int devIdx = 0; devIdx < numDevices; devIdx++) {
    TP_NVML_CHECK(
        nvmlLib, nvmlLib.deviceGetHandleByIndex_v2(devIdx, &devices[devIdx]));

    // NVML_DEVICE_UUID_V2_BUFFER_SIZE was introduced in CUDA 11.0.
#ifdef NVML_DEVICE_UUID_V2_BUFFER_SIZE
    std::array<char, NVML_DEVICE_UUID_V2_BUFFER_SIZE> uuid;
#else
    std::array<char, NVML_DEVICE_UUID_BUFFER_SIZE> uuid;
#endif
    TP_NVML_CHECK(
        nvmlLib,
        nvmlLib.deviceGetUUID(devices[devIdx], uuid.data(), uuid.size()));
    std::string uuidStr(uuid.data());
    TP_THROW_ASSERT_IF(uuidStr.substr(0, 4) != "GPU-")
        << "Couldn't obtain valid UUID for GPU #" << devIdx
        << " from CUDA driver. Got: " << uuidStr;
    uuidStr = uuidStr.substr(4);
    TP_THROW_ASSERT_IF(!isValidUuid(uuidStr))
        << "Couldn't obtain valid UUID for GPU #" << devIdx
        << " from NVML. Got: " << uuidStr;
    uuids[devIdx] = std::move(uuidStr);
  }

  std::vector<std::vector<bool>> p2pSupport(numDevices);
  for (int devIdx = 0; devIdx < numDevices; devIdx++) {
    p2pSupport[devIdx].resize(numDevices);
    for (int otherDevIdx = 0; otherDevIdx < numDevices; otherDevIdx++) {
      if (devIdx == otherDevIdx) {
        p2pSupport[devIdx][otherDevIdx] = true;
        continue;
      }
      nvmlGpuP2PStatus_t p2pStatus;
      TP_NVML_CHECK(
          nvmlLib,
          nvmlLib.deviceGetP2PStatus(
              devices[devIdx],
              devices[otherDevIdx],
              NVML_P2P_CAPS_INDEX_READ,
              &p2pStatus));
      p2pSupport[devIdx][otherDevIdx] = (p2pStatus == NVML_P2P_STATUS_OK);
    }
  }

  return std::make_tuple(std::move(uuids), std::move(p2pSupport));
}

struct DomainDescriptor {
  std::string bootId;
  std::vector<std::string> gpuUuids;
  NOP_STRUCTURE(DomainDescriptor, bootId, gpuUuids);
};

std::tuple<std::string, std::vector<std::string>, std::string>
getBootIdAndVisibleUuidsAndDomainDescriptor(const CudaLib& cudaLib) {
  NopHolder<DomainDescriptor> nopHolder;
  DomainDescriptor& domainDescriptor = nopHolder.getObject();

  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  domainDescriptor.bootId = std::move(bootID.value());

  int deviceCount;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.deviceGetCount(&deviceCount));
  domainDescriptor.gpuUuids.resize(deviceCount);
  for (int i = 0; i < deviceCount; ++i) {
    domainDescriptor.gpuUuids[i] = getUuidOfCudaDev(cudaLib, i);
  }

  std::string domainDescriptorStr = saveDescriptor(nopHolder);
  return std::make_tuple(
      std::move(domainDescriptor.bootId),
      std::move(domainDescriptor.gpuUuids),
      std::move(domainDescriptorStr));
}

std::vector<int> mapUuidsToGlobalIndices(
    const std::vector<std::string>& uuids,
    const std::vector<std::string>& globalUuids) {
  std::vector<int> res(uuids.size());
  for (int devIdx = 0; devIdx < uuids.size(); devIdx++) {
    auto iter =
        std::find(globalUuids.begin(), globalUuids.end(), uuids[devIdx]);
    TP_THROW_ASSERT_IF(iter == globalUuids.end())
        << "Couldn't find GPU #" << devIdx << " with UUID " << uuids[devIdx];
    res[devIdx] = iter - globalUuids.begin();
  }
  return res;
}

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create() {
  Error error;
  CudaLib cudaLib;
  std::tie(error, cudaLib) = CudaLib::create();
  if (error) {
    TP_VLOG(5)
        << "CUDA IPC channel is not viable because libcuda could not be loaded: "
        << error.what();
    return std::make_shared<ContextImpl>();
  }

  NvmlLib nvmlLib;
  std::tie(error, nvmlLib) = NvmlLib::create();
  if (error) {
    TP_VLOG(5)
        << "CUDA IPC channel is not viable because libnvidia-ml could not be loaded: "
        << error.what();
    return std::make_shared<ContextImpl>();
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
      TP_VLOG(4) << "CUDA IPC channel is not viable because CUDA device " << i
                 << " does not have unified addressing";
      return std::make_shared<ContextImpl>();
    }

    // The other two compute modes are "exclusive" and "prohibited", both of
    // which prevent access from an other process.
    if (props.computeMode != cudaComputeModeDefault) {
      TP_VLOG(4) << "CUDA IPC channel is not viable because CUDA device " << i
                 << " is not in default compute mode";
      return std::make_shared<ContextImpl>();
    }
  }

  std::string bootId;
  std::vector<std::string> visibleUuids;
  std::string domainDescriptor;
  std::tie(bootId, visibleUuids, domainDescriptor) =
      getBootIdAndVisibleUuidsAndDomainDescriptor(cudaLib);
  TP_VLOG(4) << "The boot ID found by the CUDA IPC channel is " << bootId;
  TP_VLOG(4)
      << "The UUIDs of the visible GPUs found by the CUDA IPC channel are "
      << joinStrs(visibleUuids);

  std::vector<std::string> globalUuids;
  std::vector<std::vector<bool>> p2pSupport;
  std::tie(globalUuids, p2pSupport) = getGlobalUuidsAndP2pSupport(nvmlLib);
  TP_VLOG(4) << "The UUIDs of all the GPUs found by the CUDA IPC channel are "
             << joinStrs(globalUuids);
  TP_VLOG(4) << "The peer-to-peer support found by the CUDA IPC channel is "
             << formatMatrix(p2pSupport);

  std::vector<int> globalIdxOfVisibleDevices =
      mapUuidsToGlobalIndices(visibleUuids, globalUuids);

  return std::make_shared<ContextImpl>(
      std::move(domainDescriptor),
      std::move(cudaLib),
      std::move(nvmlLib),
      std::move(bootId),
      std::move(globalUuids),
      std::move(p2pSupport),
      std::move(globalIdxOfVisibleDevices));
}

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/false,
          /*domainDescriptor=*/"") {}

ContextImpl::ContextImpl(
    std::string domainDescriptor,
    CudaLib cudaLib,
    NvmlLib nvmlLib,
    std::string bootId,
    std::vector<std::string> globalUuids,
    std::vector<std::vector<bool>> p2pSupport,
    std::vector<int> globalIdxOfVisibleDevices)
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/true,
          std::move(domainDescriptor)),
      cudaLib_(std::move(cudaLib)),
      nvmlLib_(std::move(nvmlLib)),
      bootId_(std::move(bootId)),
      globalUuids_(std::move(globalUuids)),
      p2pSupport_(std::move(p2pSupport)),
      globalIdxOfVisibleDevices_(std::move(globalIdxOfVisibleDevices)) {}

std::shared_ptr<CudaChannel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(std::move(connections[0]));
}

bool ContextImpl::canCommunicateWithRemote(
    const std::string& remoteDomainDescriptor) const {
  NopHolder<DomainDescriptor> nopHolder;
  loadDescriptor(nopHolder, remoteDomainDescriptor);
  DomainDescriptor& domainDescriptorObj = nopHolder.getObject();

  if (bootId_ != domainDescriptorObj.bootId) {
    return false;
  }

  std::vector<int> globalIdxOfRemoteDevice =
      mapUuidsToGlobalIndices(domainDescriptorObj.gpuUuids, globalUuids_);

  for (int localIdx = 0; localIdx < globalIdxOfVisibleDevices_.size();
       localIdx++) {
    for (int remoteIdx = 0; remoteIdx < globalIdxOfRemoteDevice.size();
         remoteIdx++) {
      if (!p2pSupport_[globalIdxOfVisibleDevices_[localIdx]]
                      [globalIdxOfRemoteDevice[remoteIdx]] ||
          !p2pSupport_[globalIdxOfRemoteDevice[remoteIdx]]
                      [globalIdxOfVisibleDevices_[localIdx]]) {
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

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
