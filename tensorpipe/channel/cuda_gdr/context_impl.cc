/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_gdr/context_impl.h>

#include <array>
#include <functional>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cuda.h>

#include <tensorpipe/channel/cuda_gdr/channel_impl.h>
#include <tensorpipe/channel/cuda_gdr/error.h>
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

} // namespace

IbvNic::IbvNic(
    std::string id,
    std::string name,
    IbvLib::device& device,
    IbvLib& ibvLib,
    CudaLib& cudaLib)
    : id_(std::move(id)),
      name_(std::move(name)),
      cudaLib_(cudaLib),
      ibvLib_(ibvLib) {
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

    std::function<void(const Error&)> cb = std::move(iter->second);
    requestsInFlight_.erase(iter);

    if (wc.status != IbvLib::WC_SUCCESS) {
      cb(TP_CREATE_ERROR(IbvError, ibvLib_.wc_status_str(wc.status)));
    } else {
      cb(Error::kSuccess);
    }

    switch (wc.opcode) {
      case IbvLib::WC_RECV:
        numRecvs++;
        break;
      case IbvLib::WC_SEND:
        numSends++;
        break;
      default:
        TP_THROW_ASSERT() << "Unknown opcode: " << wc.opcode;
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
    IbvLib::send_wr& wr,
    std::function<void(const Error&)> cb) {
  TP_DCHECK_EQ(wr.wr_id, 0);
  if (numAvailableSendSlots_ > 0) {
    wr.wr_id = nextRequestId_++;
    IbvLib::send_wr* badWr = nullptr;
    TP_VLOG(6) << "Channel context " << id_ << " posting send on device "
               << name_ << " for QP " << qp->qp_num;
    TP_CHECK_IBV_INT(ibvLib_.post_send(qp.get(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableSendSlots_--;
    requestsInFlight_.emplace(wr.wr_id, std::move(cb));
  } else {
    TP_VLOG(6) << "Channel context " << id_ << " queueing up send on device "
               << name_ << " for QP " << qp->qp_num;
    sendsWaitingForSlots_.emplace_back(qp, wr, std::move(cb));
  }
}

void IbvNic::postRecv(
    IbvQueuePair& qp,
    IbvLib::recv_wr& wr,
    std::function<void(const Error&)> cb) {
  TP_DCHECK_EQ(wr.wr_id, 0);
  if (numAvailableRecvSlots_ > 0) {
    wr.wr_id = nextRequestId_++;
    IbvLib::recv_wr* badWr = nullptr;
    TP_VLOG(6) << "Channel context " << id_ << " posting recv on device "
               << name_ << " for QP " << qp->qp_num;
    TP_CHECK_IBV_INT(ibvLib_.post_recv(qp.get(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableRecvSlots_--;
    requestsInFlight_.emplace(wr.wr_id, std::move(cb));
  } else {
    TP_VLOG(6) << "Channel context " << id_ << " queueing up recv on device "
               << name_ << " for QP " << qp->qp_num;
    recvsWaitingForSlots_.emplace_back(qp, wr, std::move(cb));
  }
}

IbvMemoryRegion& IbvNic::registerMemory(CudaBuffer buffer) {
  auto key = std::make_tuple(
      reinterpret_cast<uintptr_t>(buffer.ptr),
      static_cast<size_t>(buffer.length));

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

ContextImpl::ContextImpl(std::vector<std::string> gpuIdxToNicName)
    : ContextImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>("*") {
  Error error;

  std::tie(error, cudaLib_) = CudaLib::create();
  // FIXME Instead of throwing away the error and setting a bool, we should have
  // a way to set the context in an error state, and use that for viability.
  if (error) {
    TP_VLOG(6) << "Channel context " << id_
               << " is not viable because libcuda could not be loaded";
    return;
  }
  foundCudaLib_ = true;

  std::tie(error, ibvLib_) = IbvLib::create();
  // FIXME Instead of throwing away the error and setting a bool, we should have
  // a way to set the reactor in an error state, and use that for viability.
  if (error) {
    TP_VLOG(6) << "Channel context " << id_
               << " couldn't open libibverbs: " << error.what();
    return;
  }
  foundIbvLib_ = true;

  // TODO Check whether the NVIDIA memory peering kernel module is available.
  // And maybe even allocate and register some CUDA memory to ensure it works.

  std::unordered_set<std::string> nicNames;
  for (const auto& nicName : gpuIdxToNicName) {
    nicNames.insert(nicName);
  }

  IbvDeviceList deviceList(getIbvLib());
  std::unordered_map<std::string, size_t> nicNameToNicIdx;
  // The device index is among all available devices, the NIC index is among the
  // ones we will use.
  size_t nicIdx = 0;
  for (size_t deviceIdx = 0; deviceIdx < deviceList.size(); deviceIdx++) {
    IbvLib::device& device = deviceList[deviceIdx];
    std::string deviceName(TP_CHECK_IBV_PTR(ibvLib_.get_device_name(&device)));
    auto iter = nicNames.find(deviceName);
    if (iter != nicNames.end()) {
      TP_VLOG(5) << "Channel context " << id_ << " is using InfiniBand NIC "
                 << deviceName << " as device #" << nicIdx;
      ibvNics_.emplace_back(id_, *iter, device, ibvLib_, cudaLib_);
      nicNameToNicIdx[*iter] = nicIdx;
      nicIdx++;
      nicNames.erase(iter);
    }
  }
  TP_THROW_ASSERT_IF(!nicNames.empty())
      << "Couldn't find all the devices I was supposed to use";

  for (size_t gpuIdx = 0; gpuIdx < gpuIdxToNicName.size(); gpuIdx++) {
    gpuToNic_.push_back(nicNameToNicIdx[gpuIdxToNicName[gpuIdx]]);
  }

  startThread("TP_CUDA_GDR_loop");
}

const std::vector<size_t>& ContextImpl::getGpuToNicMapping() {
  return gpuToNic_;
}

IbvLib& ContextImpl::getIbvLib() {
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

void ContextImpl::closeImpl() {
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

std::shared_ptr<CudaChannel> ContextImpl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint /* unused */) {
  return createChannelInternal(std::move(connection));
}

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
