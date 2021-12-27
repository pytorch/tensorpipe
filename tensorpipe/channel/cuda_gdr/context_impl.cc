/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_gdr/context_impl.h>

#include <array>
#include <climits>
#include <cstdlib>
#include <functional>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cuda.h>
#include <cuda_runtime.h>

#include <tensorpipe/channel/cuda_gdr/channel_impl.h>
#include <tensorpipe/channel/cuda_gdr/error.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

namespace {

// NOTE: This is an incomplete implementation of C++17's `std::apply`.
// It's intended to only work for methods of IbvNic.
template <class TMethod, class TArgsTuple, std::size_t... I>
auto applyFuncImpl(
    IbvNic& subject,
    TMethod&& method,
    TArgsTuple&& args,
    std::index_sequence<I...> /* unused */) {
  return ((subject).*(method))(std::get<I>(std::forward<TArgsTuple>(args))...);
}

template <class TMethod, class TArgsTuple>
auto applyFunc(IbvNic& subject, TMethod&& method, TArgsTuple&& args) {
  return applyFuncImpl(
      subject,
      std::forward<TMethod>(method),
      std::forward<TArgsTuple>(args),
      std::make_index_sequence<
          std::tuple_size<std::remove_reference_t<TArgsTuple>>::value>{});
}

// We can only pass CUDA pointers to InfiniBand (for example when registering
// some memory) if InfiniBand "knows about" CUDA. Those pointers refer to the
// section of the process's virtual address space that is being used by CUDA to
// represent device memory (as part of CUDA's unified memory approach). Thus
// InfiniBand needs to talk to CUDA to translate those pointers to physical PCIe
// hardware addresses.
// This is achieved by CUDA providing a so-called "peer memory client" and
// registering it with the InfiniBand kernel module. The peer memory client is
// itself a kernel module, see https://github.com/Mellanox/nv_peer_memory.
// The "catch" is that the whole "peer memory client" system is not part of the
// official Linux InfiniBand. It's provided by a Mellanox extension, and it's
// part of their "OpenFabrics Enterprise Distribution" (MLNX_OFED), see
// https://www.mellanox.com/products/infiniband-drivers/linux/mlnx_ofed. (In
// particular, on Ubuntu, this seems to be provided by the mlnx-ofed-kernel-dkms
// package). Note that this difference between "vanilla" InfiniBand and OFED is
// only in kernel space; from our perspective the two have the same API. Also
// note that Mellanox has tried at least a couple of time to upstream this, but
// apparently without success:
// https://lore.kernel.org/linux-rdma/1412602019-30659-1-git-send-email-yishaih@mellanox.com/
// https://lore.kernel.org/linux-rdma/1455207177-11949-1-git-send-email-artemyko@mellanox.com/
// The check we use to verify if the peer memory client is active is the same as
// NCCL's one, see
// https://github.com/NVIDIA/nccl/blob/ca8485b0d01ca6dfa02f4454932011e68b461175/src/transport/net_ib.cc#L216-L230
// Whereas TensorFlow does it slightly differently, see
// https://github.com/tensorflow/networking/blob/671e2548b602f93a6c6502432b8bc131b5cc4914/tensorflow_networking/gdr/gdr_memory_manager.cc#L43-L60
static std::string kNvMemModulePath =
    "/sys/kernel/mm/memory_peers/nv_mem/version";
static std::string kNvidiaPeermemModulePath =
    "/sys/kernel/mm/memory_peers/nvidia-peermem/version";

bool isNvidiaPeerMemoryClientActive() {
  int rv1 = ::access(kNvMemModulePath.c_str(), F_OK);
  int rv2 = ::access(kNvidiaPeermemModulePath.c_str(), F_OK);
  return rv1 >= 0 || rv2 >= 0;
}

// The PCI topology is a tree, with the root being the host bridge, the leaves
// being the devices, and the other nodes being switches. We want to match each
// GPU to the InfiniBand NIC with which it shares the longest "prefix" in this
// tree, as that will route the data transfer away from the most "central"
// switches and from the host bridge. We extract the "path" of a device in the
// PCI tree by obtaining its "canonical" path in Linux's sysfs, which contains
// one component for each other device that is traversed. The format of such a
// path is /sys/devices/pci0123:45(/0123:45:67.8)+");
// See https://www.kernel.org/doc/ols/2005/ols2005v1-pages-321-334.pdf for more
// info on sysfs.

const std::string kPciPathPrefix = "/sys/devices/pci";

std::string getPciPathForIbvNic(const std::string& nicName) {
  std::array<char, PATH_MAX> pciPath;
  char* rv = ::realpath(
      ("/sys/class/infiniband/" + nicName + "/device").c_str(), pciPath.data());
  TP_THROW_SYSTEM_IF(rv == nullptr, errno);
  TP_DCHECK(rv == pciPath.data());

  std::string res(pciPath.data());
  TP_DCHECK(res.substr(0, kPciPathPrefix.size()) == kPciPathPrefix)
      << "Bad PCI path for InfiniBand NIC " << nicName << ": " << res;
  return res;
}

std::string getPciPathForGpu(int gpuIdx) {
  // The CUDA documentation says the ID will consist of a domain (16 bits), a
  // bus (8 bits), a device (5 bits) and a function (3 bits). When represented
  // as hex, including the separators and the null terminator, this takes up 13
  // bytes. However NCCL seems to suggests that sometimes the domain takes twice
  // that size, and hence 17 bytes are necessary.
  // https://github.com/NVIDIA/nccl/blob/c6dbdb00849027b4e2c277653cbef53729f7213d/src/misc/utils.cc#L49-L53
  std::array<char, 17> pciDeviceId;
  TP_CUDA_CHECK(
      cudaDeviceGetPCIBusId(pciDeviceId.data(), pciDeviceId.size(), gpuIdx));

  // Fun fact: CUDA seems to format hex letters as uppercase, but Linux's sysfs
  // expects them as lowercase.
  for (char& c : pciDeviceId) {
    if ('A' <= c && c <= 'F') {
      c = c - 'A' + 'a';
    }
  }

  std::array<char, PATH_MAX> pciPath;
  char* rv = ::realpath(
      ("/sys/bus/pci/devices/" + std::string(pciDeviceId.data())).c_str(),
      pciPath.data());
  TP_THROW_SYSTEM_IF(rv == nullptr, errno);
  TP_DCHECK(rv == pciPath.data());

  std::string res(pciPath.data());
  TP_DCHECK(res.substr(0, kPciPathPrefix.size()) == kPciPathPrefix)
      << "Bad PCI path for GPU #" << gpuIdx << ": " << res;
  return res;
}

size_t commonPrefixLength(const std::string& a, const std::string& b) {
  // The length of the longest common prefix is the index of the first char on
  // which the two strings differ.
  size_t maxLength = std::min(a.size(), b.size());
  for (size_t idx = 0; idx < maxLength; idx++) {
    if (a[idx] != b[idx]) {
      return idx;
    }
  }
  return maxLength;
}

std::vector<std::string> matchGpusToIbvNics(
    IbvLib& ibvLib,
    IbvDeviceList& deviceList) {
  struct NicInfo {
    std::string name;
    std::string pciPath;
  };
  std::vector<NicInfo> nicInfos;
  for (size_t deviceIdx = 0; deviceIdx < deviceList.size(); deviceIdx++) {
    IbvLib::device& device = deviceList[deviceIdx];
    std::string deviceName(TP_CHECK_IBV_PTR(ibvLib.get_device_name(&device)));
    std::string pciPath = getPciPathForIbvNic(deviceName);
    TP_VLOG(5) << "Resolved InfiniBand NIC " << deviceName << " to PCI path "
               << pciPath;
    nicInfos.push_back(NicInfo{std::move(deviceName), std::move(pciPath)});
  }

  int numGpus;
  TP_CUDA_CHECK(cudaGetDeviceCount(&numGpus));

  std::vector<std::string> gpuIdxToIbvNicName;
  for (int gpuIdx = 0; gpuIdx < numGpus; gpuIdx++) {
    std::string gpuPciPath = getPciPathForGpu(gpuIdx);
    TP_VLOG(5) << "Resolved GPU #" << gpuIdx << " to PCI path " << gpuPciPath;
    ssize_t bestMatchLength = -1;
    const std::string* bestMatchName = nullptr;
    for (const auto& nicInfo : nicInfos) {
      ssize_t matchLength = commonPrefixLength(gpuPciPath, nicInfo.pciPath);
      if (matchLength > bestMatchLength) {
        bestMatchLength = matchLength;
        bestMatchName = &nicInfo.name;
      }
    }
    TP_DCHECK_GE(bestMatchLength, 0);
    TP_DCHECK(bestMatchName != nullptr);
    gpuIdxToIbvNicName.push_back(*bestMatchName);
  }

  return gpuIdxToIbvNicName;
}

// In GpuDirect, the way an InfiniBand NIC accesses the GPU's memory is by
// issuing a PCIe read to some address within the GPU's "base address register"
// (BAR), i.e., a slice of the "physical" PCIe address space that belongs to the
// GPU. BARs in principle provide only "windows" into a device's memory, and
// could be re-mapped over time. When a CUDA allocation is registered on
// InfiniBand, its backing memory is mapped into the BAR and its address is
// given to the InfiniBand driver. That mapping must remain in place until the
// registration is destroyed. See
// https://docs.nvidia.com/cuda/gpudirect-rdma/index.html#how-gpudirect-rdma-works.
// CUDA GDR doesn't work well with that, because:
// - It attempts to register the entire user allocation with InfiniBand, hence
//   allocations that exceed the BAR's size can never be transferred.
// - It "caches" (or "leaks") the InfiniBand registration, because creating it
//   is expensive, so that this can be done once and then reused. This means
//   that even if each tensor that is sent is smaller than the BAR, we'd start
//   seeing failures if their cumulative size exceeded the one of the BAR.
// On some GPUs though the BAR size spans the entire GPU memory. In such cases
// what CUDA GDR is doing should be "safe". In all other cases, however, it
// isn't, and it's better to thus disable CUDA GDR entirely in these scenarios,
// so that users end up using a fully functioning (but slower) CUDA channel.
// There are multiple BARs for each GPU, but from an experimental investigation
// it seems the one that maps to the device's memory is BAR1. The programmatic
// way that the Linux kernel offers to access information about PCIe and its
// BARs is through sysfs. See
// https://www.kernel.org/doc/html/latest/PCI/sysfs-pci.html.

size_t getBar1SizeOfGpu(int gpuIdx) {
  std::string pciPath = getPciPathForGpu(gpuIdx);
  pciPath += "/resource1";

  struct stat bar1Stats;
  int rv = ::stat(pciPath.c_str(), &bar1Stats);
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  return bar1Stats.st_size;
}

bool allGpusHaveEnoughBar1Size() {
  int numGpus;
  TP_CUDA_CHECK(cudaGetDeviceCount(&numGpus));
  for (int gpuIdx = 0; gpuIdx < numGpus; gpuIdx++) {
    cudaDeviceProp gpuProps;
    TP_CUDA_CHECK(cudaGetDeviceProperties(&gpuProps, gpuIdx));
    size_t memorySize = gpuProps.totalGlobalMem;
    size_t bar1Size = getBar1SizeOfGpu(gpuIdx);
    TP_VLOG(5) << "GPU #" << gpuIdx << " has " << memorySize
               << " bytes of memory and the size of its PCIe BAR1 is "
               << bar1Size << " bytes";
    if (bar1Size < memorySize) {
      return false;
    }
  }
  return true;
}

} // namespace

IbvNic::IbvNic(
    std::string name,
    IbvLib::device& device,
    const IbvLib& ibvLib,
    const CudaLib& cudaLib)
    : name_(std::move(name)), cudaLib_(cudaLib), ibvLib_(ibvLib) {
  ctx_ = createIbvContext(ibvLib_, device);
  pd_ = createIbvProtectionDomain(ibvLib_, ctx_);
  cq_ = createIbvCompletionQueue(
      ibvLib_,
      ctx_,
      kCompletionQueueSize,
      /*cq_context=*/nullptr,
      /*channel=*/nullptr,
      /*comp_vector=*/0);
  addr_ = makeIbvAddress(ibvLib_, ctx_, kPortNum, kGlobalIdentifierIndex);
}

bool IbvNic::pollOnce() {
  std::array<IbvLib::wc, kNumPolledWorkCompletions> wcs;
  auto rv = ibvLib_.poll_cq(cq_.get(), wcs.size(), wcs.data());

  if (rv == 0) {
    return false;
  }
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  int numSends = 0;
  int numRecvs = 0;
  for (int wcIdx = 0; wcIdx < rv; wcIdx++) {
    IbvLib::wc& wc = wcs[wcIdx];

    TP_VLOG(6) << "Channel context " << id_ << " got work completion on device "
               << name_ << " for request " << wc.wr_id << " for QP "
               << wc.qp_num << " with status "
               << ibvLib_.wc_status_str(wc.status) << " and opcode "
               << ibvWorkCompletionOpcodeToStr(wc.opcode)
               << " (byte length: " << wc.byte_len << ")";

    auto iter = requestsInFlight_.find(wc.wr_id);
    TP_THROW_ASSERT_IF(iter == requestsInFlight_.end())
        << "Got work completion with unknown ID " << wc.wr_id;

    IbvLib::wc_opcode opcode = std::move(std::get<0>(iter->second));
    std::function<void(const Error&)> cb = std::move(std::get<1>(iter->second));
    requestsInFlight_.erase(iter);

    if (wc.status != IbvLib::WC_SUCCESS) {
      cb(TP_CREATE_ERROR(IbvError, ibvLib_.wc_status_str(wc.status)));
    } else {
      cb(Error::kSuccess);
    }

    switch (opcode) {
      case IbvLib::WC_RECV:
        numRecvs++;
        break;
      case IbvLib::WC_SEND:
        numSends++;
        break;
      default:
        TP_THROW_ASSERT() << "Unknown opcode: " << opcode;
    }
  }

  numAvailableSendSlots_ += numSends;
  while (!sendsWaitingForSlots_.empty() && numAvailableSendSlots_ > 0) {
    applyFunc(
        *this, &IbvNic::postSend, std::move(sendsWaitingForSlots_.front()));
    sendsWaitingForSlots_.pop_front();
  }

  numAvailableRecvSlots_ += numRecvs;
  while (!recvsWaitingForSlots_.empty() && numAvailableRecvSlots_ > 0) {
    applyFunc(
        *this, &IbvNic::postRecv, std::move(recvsWaitingForSlots_.front()));
    recvsWaitingForSlots_.pop_front();
  }

  return true;
}

void IbvNic::postSend(
    IbvQueuePair& qp,
    SendInfo info,
    std::function<void(const Error&)> cb) {
  if (numAvailableSendSlots_ > 0) {
    IbvLib::sge list;
    list.addr = reinterpret_cast<uint64_t>(info.addr);
    list.length = info.length;
    list.lkey = info.lkey;

    IbvLib::send_wr wr;
    std::memset(&wr, 0, sizeof(wr));
    wr.wr_id = nextRequestId_++;
    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode = IbvLib::WR_SEND;

    IbvLib::send_wr* badWr = nullptr;
    TP_VLOG(6) << "Channel context " << id_ << " posting send on device "
               << name_ << " for QP " << qp->qp_num;
    TP_CHECK_IBV_INT(ibvLib_.post_send(qp.get(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableSendSlots_--;
    requestsInFlight_.emplace(
        wr.wr_id, std::make_tuple(IbvLib::WC_SEND, std::move(cb)));
  } else {
    TP_VLOG(6) << "Channel context " << id_ << " queueing up send on device "
               << name_ << " for QP " << qp->qp_num;
    sendsWaitingForSlots_.emplace_back(qp, info, std::move(cb));
  }
}

void IbvNic::postRecv(
    IbvQueuePair& qp,
    RecvInfo info,
    std::function<void(const Error&)> cb) {
  if (numAvailableRecvSlots_ > 0) {
    IbvLib::sge list;
    list.addr = reinterpret_cast<uint64_t>(info.addr);
    list.length = info.length;
    list.lkey = info.lkey;

    IbvLib::recv_wr wr;
    std::memset(&wr, 0, sizeof(wr));
    wr.wr_id = nextRequestId_++;
    wr.sg_list = &list;
    wr.num_sge = 1;

    IbvLib::recv_wr* badWr = nullptr;
    TP_VLOG(6) << "Channel context " << id_ << " posting recv on device "
               << name_ << " for QP " << qp->qp_num;
    TP_CHECK_IBV_INT(ibvLib_.post_recv(qp.get(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableRecvSlots_--;
    requestsInFlight_.emplace(
        wr.wr_id, std::make_tuple(IbvLib::WC_RECV, std::move(cb)));
  } else {
    TP_VLOG(6) << "Channel context " << id_ << " queueing up recv on device "
               << name_ << " for QP " << qp->qp_num;
    recvsWaitingForSlots_.emplace_back(qp, info, std::move(cb));
  }
}

IbvMemoryRegion& IbvNic::registerMemory(CudaBuffer buffer) {
  // FIXME Instead of re-querying the device, have the caller provide it.
  CudaDeviceGuard guard(cudaDeviceForPointer(cudaLib_, buffer.ptr));

  CUdeviceptr basePtr;
  size_t allocSize;
  TP_CUDA_DRIVER_CHECK(
      cudaLib_,
      cudaLib_.memGetAddressRange(
          &basePtr, &allocSize, reinterpret_cast<CUdeviceptr>(buffer.ptr)));

  unsigned long long bufferId;
  TP_CUDA_DRIVER_CHECK(
      cudaLib_,
      cudaLib_.pointerGetAttribute(
          &bufferId, CU_POINTER_ATTRIBUTE_BUFFER_ID, basePtr));

  auto iter = memoryRegions_.find(bufferId);
  if (iter != memoryRegions_.end()) {
    return iter->second;
  }
  std::tie(iter, std::ignore) = memoryRegions_.emplace(
      bufferId,
      createIbvMemoryRegion(
          ibvLib_,
          pd_,
          reinterpret_cast<void*>(basePtr),
          allocSize,
          IbvLib::ACCESS_LOCAL_WRITE));
  return iter->second;
}

bool IbvNic::readyToClose() const {
  return requestsInFlight_.empty();
}

void IbvNic::setId(std::string id) {
  id_ = std::move(id);
}

std::shared_ptr<ContextImpl> ContextImpl::create(
    optional<std::vector<std::string>> gpuIdxToNicName) {
  Error error;

  CudaLib cudaLib;
  std::tie(error, cudaLib) = CudaLib::create();
  // FIXME Instead of throwing away the error and setting a bool, we should have
  // a way to set the context in an error state, and use that for viability.
  if (error) {
    TP_VLOG(5)
        << "CUDA GDR channel is not viable because libcuda could not be loaded: "
        << error.what();
    return nullptr;
  }

  IbvLib ibvLib;
  std::tie(error, ibvLib) = IbvLib::create();
  // FIXME Instead of throwing away the error and setting a bool, we should have
  // a way to set the context in an error state, and use that for viability.
  if (error) {
    TP_VLOG(5)
        << "CUDA GDR channel is not viable because libibverbs could not be loaded: "
        << error.what();
    return nullptr;
  }

  if (!isNvidiaPeerMemoryClientActive()) {
    TP_VLOG(5)
        << "CUDA GDR channel is not viable because the nv_peer_mem kernel module isn't active";
    return nullptr;
  }

  IbvDeviceList deviceList;
  std::tie(error, deviceList) = IbvDeviceList::create(ibvLib);
  if (error && error.isOfType<SystemError>() &&
      error.castToType<SystemError>()->errorCode() == ENOSYS) {
    TP_VLOG(5)
        << "CUDA GDR channel couldn't get list of InfiniBand devices because the kernel module isn't "
        << "loaded";
    return nullptr;
  }
  TP_THROW_ASSERT_IF(error)
      << "Couldn't get list of InfiniBand devices: " << error.what();
  if (deviceList.size() == 0) {
    TP_VLOG(5)
        << "CUDA GDR channel is not viable because it couldn't find any InfiniBand NICs";
    return nullptr;
  }

  // FIXME In principle we could just exclude the GPUs that violate this check
  // but keep working with the other ones (if any).
  if (!allGpusHaveEnoughBar1Size()) {
    TP_VLOG(5)
        << "CUDA GDR channel is not viable because some GPUs don't have a large enough PCIe BAR1 size";
    return nullptr;
  }

  std::unordered_map<Device, std::string> deviceDescriptors;
  for (const auto& device : getCudaDevices(cudaLib)) {
    deviceDescriptors[device] = "*";
  }

  return std::make_shared<ContextImpl>(
      std::move(deviceDescriptors),
      std::move(cudaLib),
      std::move(ibvLib),
      std::move(deviceList),
      std::move(gpuIdxToNicName));
}

ContextImpl::ContextImpl(
    std::unordered_map<Device, std::string> deviceDescriptors,
    CudaLib cudaLib,
    IbvLib ibvLib,
    IbvDeviceList deviceList,
    optional<std::vector<std::string>> gpuIdxToNicName)
    : ContextImplBoilerplate<ContextImpl, ChannelImpl>(
          std::move(deviceDescriptors)),
      cudaLib_(std::move(cudaLib)),
      ibvLib_(std::move(ibvLib)) {
  std::vector<std::string> actualGpuIdxToNicName;
  if (gpuIdxToNicName.has_value()) {
    int numGpus;
    TP_CUDA_CHECK(cudaGetDeviceCount(&numGpus));
    TP_THROW_ASSERT_IF(numGpus != gpuIdxToNicName->size())
        << "The mapping from GPUs to InfiniBand NICs contains an unexpected "
        << "number of items: found " << gpuIdxToNicName->size() << ", expected "
        << numGpus;

    actualGpuIdxToNicName = std::move(gpuIdxToNicName.value());
  } else {
    actualGpuIdxToNicName = matchGpusToIbvNics(ibvLib, deviceList);
  }

  for (int gpuIdx = 0; gpuIdx < actualGpuIdxToNicName.size(); gpuIdx++) {
    TP_VLOG(5) << "CUDA GDR channel mapped GPU #" << gpuIdx
               << " to InfiniBand NIC " << actualGpuIdxToNicName[gpuIdx];
  }

  std::unordered_set<std::string> nicNames;
  for (const auto& nicName : actualGpuIdxToNicName) {
    nicNames.insert(nicName);
  }

  std::unordered_map<std::string, size_t> nicNameToNicIdx;
  // The device index is among all available devices, the NIC index is among the
  // ones we will use.
  size_t nicIdx = 0;
  for (size_t deviceIdx = 0; deviceIdx < deviceList.size(); deviceIdx++) {
    IbvLib::device& device = deviceList[deviceIdx];
    std::string deviceName(TP_CHECK_IBV_PTR(ibvLib.get_device_name(&device)));
    auto iter = nicNames.find(deviceName);
    if (iter != nicNames.end()) {
      TP_VLOG(5) << "CUDA GDR channel is using InfiniBand NIC " << deviceName
                 << " as device #" << nicIdx;
      ibvNics_.emplace_back(*iter, device, ibvLib_, cudaLib_);
      nicNameToNicIdx[*iter] = nicIdx;
      nicIdx++;
      nicNames.erase(iter);
    }
  }
  TP_THROW_ASSERT_IF(!nicNames.empty())
      << "Couldn't find all the devices I was supposed to use";

  for (size_t gpuIdx = 0; gpuIdx < actualGpuIdxToNicName.size(); gpuIdx++) {
    gpuToNic_.push_back(nicNameToNicIdx[actualGpuIdxToNicName[gpuIdx]]);
  }

  startThread("TP_CUDA_GDR_loop");
}

const CudaLib& ContextImpl::getCudaLib() {
  return cudaLib_;
}

const std::vector<size_t>& ContextImpl::getGpuToNicMapping() {
  return gpuToNic_;
}

const IbvLib& ContextImpl::getIbvLib() {
  return ibvLib_;
}

IbvNic& ContextImpl::getIbvNic(size_t nicIdx) {
  TP_DCHECK_LT(nicIdx, ibvNics_.size());
  return ibvNics_[nicIdx];
}

bool ContextImpl::pollOnce() {
  for (IbvNic& ibvNic : ibvNics_) {
    if (ibvNic.pollOnce()) {
      return true;
    }
  }
  return pollCudaOnce();
}

bool ContextImpl::pollCudaOnce() {
  bool any = false;
  for (auto iter = pendingCudaEvents_.begin(); iter != pendingCudaEvents_.end();
       iter++) {
    const CudaEvent& event = std::get<0>(*iter);

    if (event.query()) {
      std::function<void(const Error&)> cb = std::move(std::get<1>(*iter));
      cb(Error::kSuccess);
      iter = pendingCudaEvents_.erase(iter);
      any = true;
    }
  }
  return any;
}

void ContextImpl::waitForCudaEvent(
    const CudaEvent& event,
    std::function<void(const Error&)> cb) {
  deferToLoop([this, &event, cb{std::move(cb)}]() mutable {
    waitForCudaEventFromLoop(event, std::move(cb));
  });
}

void ContextImpl::waitForCudaEventFromLoop(
    const CudaEvent& event,
    std::function<void(const Error&)> cb) {
  TP_DCHECK(inLoop());

  pendingCudaEvents_.emplace_back(event, std::move(cb));
}

bool ContextImpl::readyToClose() {
  for (const IbvNic& ibvNic : ibvNics_) {
    if (!ibvNic.readyToClose()) {
      return false;
    }
  }
  return pendingCudaEvents_.empty();
}

void ContextImpl::handleErrorImpl() {
  stopBusyPolling();
}

void ContextImpl::joinImpl() {
  joinThread();

  // FIXME It would be nice if this could be done by the thread itself just
  // before it returns, rather than by the user.
  ibvNics_.clear();
}

void ContextImpl::setIdImpl() {
  for (IbvNic& ibvNic : ibvNics_) {
    ibvNic.setId(id_);
  }
}

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

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
