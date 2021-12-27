/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include <unistd.h>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cuda_ipc/channel_impl.h>
#include <tensorpipe/channel/cuda_ipc/constants.h>
#include <tensorpipe/channel/helpers.h>
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
  int64_t pid;
  std::string deviceUuid;
  NOP_STRUCTURE(DeviceDescriptor, bootId, pid, deviceUuid);
};

DeviceDescriptor deserializeDeviceDescriptor(
    const std::string& deviceDescriptor) {
  NopHolder<DeviceDescriptor> nopHolder;
  loadDescriptor(nopHolder, deviceDescriptor);
  return std::move(nopHolder.getObject());
}

std::string generateBootId() {
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  return bootID.value();
}

// FIXME We'd want this to return a std::vector<CudaEvent>, but CudaEvents
// aren't default-constructible nor movable. Hence either we make them such,
// or we use some pointer magic (like placement new). For now, we work around
// this by using a unique_ptr and wrapping them in optional<>, but it's silly.
std::unique_ptr<optional<CudaEvent>[]> createIpcEventArray(
    int deviceIdx,
    size_t numEvents) {
  auto events = std::make_unique<optional<CudaEvent>[]>(numEvents);
  // The CUDA driver has a bug where creating and/or destroying IPC events
  // sometimes causes a deadlock (it's unclear which of the two steps is the
  // cause). The deadlock tends to manifest as a cudaStreamSynchronize call
  // never returning. Just to be safe, and to catch such a deadlock early and
  // clearly, let's add extra syncs here. (The bug is fixed in v460).
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaDeviceSynchronize());
  }
  for (size_t idx = 0; idx < numEvents; idx++) {
    events[idx].emplace(deviceIdx, true);
    // One day we might get tempted to have CudaEvent lazily initialize its
    // cudaEvent_t, just like PyTorch does. However here we explicitly want to
    // eagerly initialize IPC events, as creating them late might deadlock with
    // old CUDA driver versions. This check should hopefully catch if the event
    // is lazy-initialized.
    TP_THROW_ASSERT_IF(events[idx]->raw() == nullptr);
  }
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaDeviceSynchronize());
  }
  return events;
}

std::vector<cudaIpcEventHandle_t> getIpcHandlesForEventArray(
    optional<CudaEvent> events[],
    size_t numEvents) {
  std::vector<cudaIpcEventHandle_t> eventHandles(numEvents);
  for (size_t idx = 0; idx < numEvents; idx++) {
    eventHandles[idx] = events[idx]->getIpcHandle();
  }
  return eventHandles;
}

} // namespace

ContextImpl::Outbox::Outbox(int deviceIdx)
    : buffer(kStagingAreaSize, deviceIdx),
      events(createIpcEventArray(deviceIdx, kNumSlots)),
      handle(this->buffer.getIpcHandle()),
      eventHandles(getIpcHandlesForEventArray(this->events.get(), kNumSlots)),
      allocator(this->buffer.ptr(), kNumSlots, kSlotSize) {}

ContextImpl::Outbox::~Outbox() {
  // The CUDA driver has a bug where creating and/or destroying IPC events
  // sometimes causes a deadlock (it's unclear which of the two steps is the
  // cause). The deadlock tends to manifest as a cudaStreamSynchronize call
  // never returning. Just to be safe, and to catch such a deadlock early and
  // clearly, let's add extra syncs here. (The bug is fixed in v460).
  {
    CudaDeviceGuard guard(buffer.deviceIdx());
    TP_CUDA_CHECK(cudaDeviceSynchronize());
  }
  events.reset();
  {
    CudaDeviceGuard guard(buffer.deviceIdx());
    TP_CUDA_CHECK(cudaDeviceSynchronize());
  }
}

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
  const pid_t pid = ::getpid();

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
    deviceDescriptor.pid = static_cast<int64_t>(pid);
    deviceDescriptor.deviceUuid = getUuidOfDevice(cudaLib, device.index);

    deviceDescriptors[device] = saveDescriptor(nopHolder);
  }

  std::vector<std::string> globalUuids;
  std::vector<std::vector<bool>> p2pSupport;
  std::tie(globalUuids, p2pSupport) = getGlobalUuidsAndP2pSupport(nvmlLib);
  TP_VLOG(4) << "The UUIDs of all the GPUs found by the CUDA IPC channel are "
             << joinStrs(globalUuids);
  TP_VLOG(4) << "The peer-to-peer support found by the CUDA IPC channel is "
             << formatMatrix(p2pSupport);

  std::ostringstream oss;
  optional<std::string> nsId = getLinuxNamespaceId(LinuxNamespace::kPid);
  if (!nsId.has_value()) {
    TP_VLOG(4)
        << "CUDA IPC channel is not viable because it couldn't determine the PID namespace ID";
    return nullptr;
  }
  oss << nsId.value() << "_" << pid;
  std::string processIdentifier = oss.str();

  return std::make_shared<ContextImpl>(
      std::move(deviceDescriptors),
      std::move(cudaLib),
      std::move(nvmlLib),
      std::move(globalUuids),
      std::move(p2pSupport),
      std::move(processIdentifier));
}

ContextImpl::ContextImpl(
    std::unordered_map<Device, std::string> deviceDescriptors,
    CudaLib cudaLib,
    NvmlLib nvmlLib,
    std::vector<std::string> globalUuids,
    std::vector<std::vector<bool>> p2pSupport,
    std::string processIdentifier)
    : ContextImplBoilerplate<ContextImpl, ChannelImpl>(
          std::move(deviceDescriptors)),
      cudaLib_(std::move(cudaLib)),
      nvmlLib_(std::move(nvmlLib)),
      globalUuids_(std::move(globalUuids)),
      p2pSupport_(std::move(p2pSupport)),
      processIdentifier_(processIdentifier) {}

std::shared_ptr<Channel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(
      std::move(connections[0]), std::move(connections[1]));
}

size_t ContextImpl::numConnectionsNeeded() const {
  // The control connection needs to carry two unrelated streams in each
  // direction (the descriptors and the replies), and it's thus simpler to just
  // use two such connections.
  return 2;
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

  // Disable CudaIpc when both endpoints are in the same process, as a CUDA IPC
  // handle cannot be opened in the same process in which it was created.
  if (nopLocalDeviceDescriptor.pid == nopRemoteDeviceDescriptor.pid) {
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

void ContextImpl::allocateSlot(
    int deviceIdx,
    size_t length,
    SlotAllocCallback callback) {
  if (outboxes_.size() <= deviceIdx) {
    outboxes_.resize(deviceIdx + 1);
  }
  if (outboxes_[deviceIdx] == nullptr) {
    outboxes_[deviceIdx] = std::make_unique<Outbox>(deviceIdx);
  }

  // We don't need to wrap this callback with the callbackWrapper_ because the
  // callback that was passed to this method already is, and because all we're
  // doing here is wrap that callback and do read-only accesses to the outbox.
  Outbox& outbox = *outboxes_[deviceIdx];
  outboxes_[deviceIdx]->allocator.alloc(
      length,
      [&outbox, callback{std::move(callback)}](
          const Error& error, Allocator::TChunk chunk) {
        if (error) {
          callback(error, 0, std::move(chunk), nullptr);
          return;
        }
        size_t slotIdx = (chunk.get() - outbox.buffer.ptr()) / kSlotSize;
        callback(
            error, slotIdx, std::move(chunk), &outbox.events[slotIdx].value());
      });
}

ContextImpl::OutboxInfo ContextImpl::getLocalOutboxInfo(int deviceIdx) {
  TP_DCHECK(outboxes_.size() > deviceIdx);
  TP_DCHECK(outboxes_[deviceIdx] != nullptr);
  OutboxInfo info;
  info.processIdentifier = processIdentifier_;
  info.memHandle = std::string(
      reinterpret_cast<const char*>(&outboxes_[deviceIdx]->handle),
      sizeof(cudaIpcMemHandle_t));
  info.eventHandles.reserve(kNumSlots);
  for (size_t slotIdx = 0; slotIdx < kNumSlots; slotIdx++) {
    info.eventHandles.emplace_back(
        reinterpret_cast<const char*>(
            &outboxes_[deviceIdx]->eventHandles[slotIdx]),
        sizeof(cudaIpcEventHandle_t));
  }
  return info;
}

const ContextImpl::RemoteOutboxHandle& ContextImpl::openRemoteOutbox(
    int localDeviceIdx,
    int remoteDeviceIdx,
    OutboxInfo remoteOutboxInfo) {
  RemoteOutboxKey key{
      std::move(remoteOutboxInfo.processIdentifier),
      remoteDeviceIdx,
      localDeviceIdx};
  decltype(remoteOutboxes_)::iterator iter;
  bool didntExist;
  std::tie(iter, didntExist) =
      remoteOutboxes_.emplace(std::move(key), RemoteOutboxHandle{});
  RemoteOutboxHandle& outbox = iter->second;

  if (didntExist) {
    CudaDeviceGuard guard(localDeviceIdx);
    outbox.buffer = CudaIpcBuffer(
        localDeviceIdx,
        *reinterpret_cast<const cudaIpcMemHandle_t*>(
            remoteOutboxInfo.memHandle.data()));
    outbox.events = std::make_unique<optional<CudaEvent>[]>(kNumSlots);
    for (size_t slotIdx = 0; slotIdx < kNumSlots; slotIdx++) {
      outbox.events[slotIdx].emplace(
          localDeviceIdx,
          *reinterpret_cast<const cudaIpcEventHandle_t*>(
              remoteOutboxInfo.eventHandles[slotIdx].data()));
    }
  }

  return outbox;
}

void ContextImpl::handleErrorImpl() {
  for (std::unique_ptr<Outbox>& outbox : outboxes_) {
    if (outbox != nullptr) {
      outbox->allocator.close();
    }
  }
}

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
