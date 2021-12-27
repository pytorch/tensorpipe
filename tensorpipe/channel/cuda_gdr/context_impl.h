/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/channel/cuda_gdr/constants.h>
#include <tensorpipe/common/busy_polling_loop.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/ibv.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

class ChannelImpl;

class IbvNic {
 public:
  IbvNic(
      std::string name,
      IbvLib::device& device,
      const IbvLib& ibvLib,
      const CudaLib& cudaLib);

  IbvProtectionDomain& getIbvPd() {
    return pd_;
  }

  IbvCompletionQueue& getIbvCq() {
    return cq_;
  }

  const IbvAddress& getIbvAddress() {
    return addr_;
  }

  struct SendInfo {
    void* addr;
    size_t length;
    uint32_t lkey;
  };

  void postSend(
      IbvQueuePair& qp,
      SendInfo info,
      std::function<void(const Error&)> cb);

  struct RecvInfo {
    void* addr;
    size_t length;
    uint32_t lkey;
  };

  void postRecv(
      IbvQueuePair& qp,
      RecvInfo info,
      std::function<void(const Error&)> cb);

  bool pollOnce();

  IbvMemoryRegion& registerMemory(CudaBuffer buffer);

  bool readyToClose() const;

  void setId(std::string id);

 private:
  // The ID of the context, for use in verbose logging.
  std::string id_{"N/A"};
  // The name of the InfiniBand device.
  const std::string name_;

  const CudaLib& cudaLib_;

  const IbvLib& ibvLib_;
  IbvContext ctx_;
  IbvProtectionDomain pd_;
  IbvCompletionQueue cq_;
  IbvAddress addr_;

  size_t numAvailableRecvSlots_ = kNumRecvs;
  std::deque<
      std::tuple<IbvQueuePair&, RecvInfo, std::function<void(const Error&)>>>
      recvsWaitingForSlots_;

  size_t numAvailableSendSlots_ = kNumSends;
  std::deque<
      std::tuple<IbvQueuePair&, SendInfo, std::function<void(const Error&)>>>
      sendsWaitingForSlots_;

  // We need one common map for both send and recv requests because in principle
  // we cannot access the opcode of a failed operation, meaning we couldn't
  // match it to its callback. However, we could group them by QP number or, in
  // fact, we could have the QP store these requests and we just wake it up when
  // a completion occurs.
  std::unordered_map<
      uint64_t,
      std::tuple<IbvLib::wc_opcode, std::function<void(const Error&)>>>
      requestsInFlight_;
  uint64_t nextRequestId_ = 0;

  // The ibverbs memory regions are indexed by the CUDA driver's buffer ID for
  // the GPU allocation, which is unique (within the process) and never reused.
  // This will prevent us from re-using the memory region if a buffer gets
  // deallocated and reallocated (although we will not clean up the old memory
  // region until we close the context).
  std::map<unsigned long long, IbvMemoryRegion> memoryRegions_;
};

class ContextImpl final
    : public BusyPollingLoop,
      public ContextImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create(
      optional<std::vector<std::string>> gpuIdxToNicName = nullopt);

  ContextImpl(
      std::unordered_map<Device, std::string> deviceDescriptors,
      CudaLib cudaLib,
      IbvLib ibvLib,
      IbvDeviceList deviceList,
      optional<std::vector<std::string>> gpuIdxToNicName);

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  size_t numConnectionsNeeded() const override;

  const CudaLib& getCudaLib();

  const std::vector<size_t>& getGpuToNicMapping();

  const IbvLib& getIbvLib();

  IbvNic& getIbvNic(size_t nicIdx);

  void waitForCudaEvent(
      const CudaEvent& event,
      std::function<void(const Error&)> cb);

 protected:
  // Implement BusyPollingLoop hooks.
  bool pollOnce() override;
  bool readyToClose() override;

  // Implement the entry points called by ContextImplBoilerplate.
  void handleErrorImpl() override;
  void joinImpl() override;
  void setIdImpl() override;

 private:
  const CudaLib cudaLib_;
  const IbvLib ibvLib_;

  std::vector<IbvNic> ibvNics_;
  std::vector<size_t> gpuToNic_;

  std::list<std::tuple<const CudaEvent&, std::function<void(const Error&)>>>
      pendingCudaEvents_;

  bool pollCudaOnce();

  void waitForCudaEventFromLoop(
      const CudaEvent& event,
      std::function<void(const Error&)> cb);
};

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
