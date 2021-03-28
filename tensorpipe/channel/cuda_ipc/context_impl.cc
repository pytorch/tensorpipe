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
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/nop.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/strings.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

namespace {

// Half of them will go in the send pool, the other half in the recv pool.
// This number is per-device.
static constexpr size_t kNumIpcEventsInPool = 1000;

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

int globalIdxForDevice(
    const std::vector<std::string>& globalUuids,
    const std::string& uuid) {
  auto iter = std::find(globalUuids.begin(), globalUuids.end(), uuid);
  TP_THROW_ASSERT_IF(iter == globalUuids.end())
      << "Couldn't find GPU with UUID " << uuid;

  return iter - globalUuids.begin();
}

struct DeviceDescriptor {
  std::string bootId;
  std::string deviceUuid;
  NOP_STRUCTURE(DeviceDescriptor, bootId, deviceUuid);
};

DeviceDescriptor deserializeDeviceDescriptor(
    const std::string& deviceDescriptor) {
  NopHolder<DeviceDescriptor> nopHolder;
  // FIXME
  // loadDescriptor(nopHolder, deviceDescriptor);
  return std::move(nopHolder.getObject());
}

std::string generateBootId() {
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  return bootID.value();
}

std::string genProcessIdentifier() {
  std::ostringstream oss;
  optional<std::string> nsId = getLinuxNamespaceId(LinuxNamespace::kPid);
  TP_DCHECK(nsId.has_value());
  oss << nsId.value() << "_" << ::getpid();
  return oss.str();
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
    return nullptr;
  }

  NvmlLib nvmlLib;
  std::tie(error, nvmlLib) = NvmlLib::create();
  if (error) {
    TP_VLOG(5)
        << "CUDA IPC channel is not viable because libnvidia-ml could not be loaded: "
        << error.what();
    return nullptr;
  }

  const std::string bootId = generateBootId();

  std::unordered_map<Device, std::string> deviceDescriptors;
  for (const auto& device : getCudaDevices(cudaLib)) {
    // This part is largely inspired from
    // https://github.com/NVIDIA/cuda-samples/blob/master/Samples/simpleIPC/simpleIPC.cu.
    cudaDeviceProp props;
    TP_CUDA_CHECK(cudaGetDeviceProperties(&props, device.index));

    // Unified addressing is required for IPC.
    if (!props.unifiedAddressing) {
      TP_VLOG(4) << "CUDA IPC channel is not viable because CUDA device "
                 << device.index << " does not have unified addressing";
      return nullptr;
    }

    // The other two compute modes are "exclusive" and "prohibited", both of
    // which prevent access from an other process.
    if (props.computeMode != cudaComputeModeDefault) {
      TP_VLOG(4) << "CUDA IPC channel is not viable because CUDA device "
                 << device.index << " is not in default compute mode";
      return nullptr;
    }

    NopHolder<DeviceDescriptor> nopHolder;
    DeviceDescriptor& deviceDescriptor = nopHolder.getObject();
    deviceDescriptor.bootId = bootId;
    deviceDescriptor.deviceUuid = getUuidOfDevice(cudaLib, device.index);

    // FIXME
    // deviceDescriptors[device] = saveDescriptor(nopHolder);
  }

  std::vector<std::string> globalUuids;
  std::vector<std::vector<bool>> p2pSupport;
  std::tie(globalUuids, p2pSupport) = getGlobalUuidsAndP2pSupport(nvmlLib);
  TP_VLOG(4) << "The UUIDs of all the GPUs found by the CUDA IPC channel are "
             << joinStrs(globalUuids);
  TP_VLOG(4) << "The peer-to-peer support found by the CUDA IPC channel is "
             << formatMatrix(p2pSupport);

  return std::make_shared<ContextImpl>(
      std::move(deviceDescriptors),
      std::move(cudaLib),
      std::move(nvmlLib),
      std::move(globalUuids),
      std::move(p2pSupport));
}

ContextImpl::ContextImpl(
    std::unordered_map<Device, std::string> deviceDescriptors,
    CudaLib cudaLib,
    NvmlLib nvmlLib,
    std::vector<std::string> globalUuids,
    std::vector<std::vector<bool>> p2pSupport)
    : ContextImplBoilerplate<ContextImpl, ChannelImpl>(
          std::move(deviceDescriptors)),
      cudaLib_(std::move(cudaLib)),
      nvmlLib_(std::move(nvmlLib)),
      globalUuids_(std::move(globalUuids)),
      p2pSupport_(std::move(p2pSupport)),
      processIdentifier_(genProcessIdentifier()) {}

std::shared_ptr<Channel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(
      std::move(connections[0]), std::move(connections[1]));
}

size_t ContextImpl::numConnectionsNeeded() const {
  // The control connection needs to carry two unrelated streams in each
  // direction (the replies and the acks), and it's thus simpler to just use two
  // such connections.
  return 2;
}

bool ContextImpl::supportsDeviceType(DeviceType type) const {
  return (DeviceType::kCuda == type);
}

bool ContextImpl::canCommunicateWithRemote(
    const std::string& localDeviceDescriptor,
    const std::string& remoteDeviceDescriptor) const {
  DeviceDescriptor nopLocalDeviceDescriptor =
      deserializeDeviceDescriptor(localDeviceDescriptor);
  DeviceDescriptor nopRemoteDeviceDescriptor =
      deserializeDeviceDescriptor(remoteDeviceDescriptor);

  if (nopLocalDeviceDescriptor.bootId != nopRemoteDeviceDescriptor.bootId) {
    return false;
  }

  int localGlobalIdx =
      globalIdxForDevice(globalUuids_, nopLocalDeviceDescriptor.deviceUuid);
  int remoteGlobalIdx =
      globalIdxForDevice(globalUuids_, nopRemoteDeviceDescriptor.deviceUuid);

  return p2pSupport_[localGlobalIdx][remoteGlobalIdx] &&
      p2pSupport_[remoteGlobalIdx][localGlobalIdx];
}

const CudaLib& ContextImpl::getCudaLib() {
  return cudaLib_;
}

const std::string& ContextImpl::getProcessIdentifier() {
  return processIdentifier_;
}

void* ContextImpl::openIpcHandle(
    std::string allocationId,
    const cudaIpcMemHandle_t& remoteHandle,
    int deviceIdx) {
  auto iter = openIpcHandles_.find(std::make_tuple(allocationId, deviceIdx));
  if (iter != openIpcHandles_.end()) {
    return iter->second;
  }
  CudaDeviceGuard guard(deviceIdx);
  void* remotePtr;
  TP_CUDA_CHECK(cudaIpcOpenMemHandle(
      &remotePtr, remoteHandle, cudaIpcMemLazyEnablePeerAccess));
  openIpcHandles_[std::make_tuple(allocationId, deviceIdx)] = remotePtr;
  return remotePtr;
}

void ContextImpl::requestSendEvent(
    int deviceIdx,
    CudaEventPool::RequestCallback callback) {
  if (error_) {
    callback(error_, nullptr);
    return;
  }
  if (sendEventPools_.size() <= deviceIdx) {
    sendEventPools_.resize(deviceIdx + 1);
  }
  if (sendEventPools_[deviceIdx] == nullptr) {
    sendEventPools_[deviceIdx] = std::make_unique<CudaEventPool>(
        kNumIpcEventsInPool / 2, deviceIdx, /*interprocess=*/true);
  }
  sendEventPools_[deviceIdx]->request(std::move(callback));
}

void ContextImpl::requestRecvEvent(
    int deviceIdx,
    CudaEventPool::RequestCallback callback) {
  if (error_) {
    callback(error_, nullptr);
    return;
  }
  if (recvEventPools_.size() <= deviceIdx) {
    recvEventPools_.resize(deviceIdx + 1);
  }
  if (recvEventPools_[deviceIdx] == nullptr) {
    recvEventPools_[deviceIdx] = std::make_unique<CudaEventPool>(
        kNumIpcEventsInPool / 2, deviceIdx, /*interprocess=*/true);
  }
  recvEventPools_[deviceIdx]->request(std::move(callback));
}

void ContextImpl::handleErrorImpl() {
  for (std::unique_ptr<CudaEventPool>& pool : sendEventPools_) {
    if (pool != nullptr) {
      pool->close();
    }
  }
  for (std::unique_ptr<CudaEventPool>& pool : recvEventPools_) {
    if (pool != nullptr) {
      pool->close();
    }
  }
}

void ContextImpl::joinImpl() {
  for (const auto& handle : openIpcHandles_) {
    int deviceIdx = std::get<1>(handle.first);
    void* remotePtr = handle.second;

    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaIpcCloseMemHandle(remotePtr));
  }
}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
