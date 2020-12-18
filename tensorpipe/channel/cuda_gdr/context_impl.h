/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <list>
#include <map>

#include <tensorpipe/channel/cuda_gdr/constants.h>
#include <tensorpipe/channel/cuda_gdr/context.h>
#include <tensorpipe/common/busy_polling_loop.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/ibv.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

class IbvNic {
 public:
  IbvNic(
      std::string id,
      std::string name,
      IbvLib::device& device,
      IbvLib& ibvLib);

  IbvProtectionDomain& getIbvPd() {
    return pd_;
  }

  IbvCompletionQueue& getIbvCq() {
    return cq_;
  }

  const IbvAddress& getIbvAddress() {
    return addr_;
  }

  void postSend(
      IbvQueuePair& qp,
      IbvLib::send_wr& wr,
      std::function<void(const Error&)> cb);

  void postRecv(
      IbvQueuePair& qp,
      IbvLib::recv_wr& wr,
      std::function<void(const Error&)> cb);

  bool pollOnce();

  IbvMemoryRegion& registerMemory(CudaBuffer buffer);

  bool readyToClose() const;

  void setId(std::string id);

 private:
  // The ID of the context, for use in verbose logging.
  std::string id_{"N/A"};
  // The name of the InfiniBand device.
  std::string name_;

  IbvLib& ibvLib_;
  IbvContext ctx_;
  IbvProtectionDomain pd_;
  IbvCompletionQueue cq_;
  IbvAddress addr_;

  size_t numAvailableRecvSlots_ = kNumRecvs;
  std::deque<std::tuple<
      IbvQueuePair&,
      IbvLib::recv_wr&,
      std::function<void(const Error&)>>>
      recvsWaitingForSlots_;

  size_t numAvailableSendSlots_ = kNumSends;
  std::deque<std::tuple<
      IbvQueuePair&,
      IbvLib::send_wr&,
      std::function<void(const Error&)>>>
      sendsWaitingForSlots_;

  // We need one common map for both send and recv requests because in principle
  // we cannot access the opcode of a failed operation, meaning we couldn't
  // match it to its callback. However, we could group them by QP number or, in
  // fact, we could have the QP store these requests and we just wake it up when
  // a completion occurs.
  std::unordered_map<uint64_t, std::function<void(const Error&)>>
      requestsInFlight_;
  uint64_t nextRequestId_ = 0;

  std::map<std::tuple<uintptr_t, size_t>, IbvMemoryRegion> memoryRegions_;
};

class Context::Impl : public BusyPollingLoop,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  explicit Impl(std::vector<std::string> gpuIdxToNicName);

  const std::string& domainDescriptor() const;

  std::shared_ptr<channel::CudaChannel> createChannel(
      std::shared_ptr<transport::Connection> connection,
      Endpoint endpoint);

  void setId(std::string id);

  ClosingEmitter& getClosingEmitter();

  const std::vector<size_t>& getGpuToNicMapping();

  IbvLib& getIbvLib();

  IbvNic& getIbvNic(size_t nicIdx);

  void waitForCudaEvent(
      const CudaEvent& event,
      std::function<void(const Error&)> cb);

  void close();

  void join();

 protected:
  // Implement BusyPollingLoop hooks.
  bool pollOnce() override;
  bool readyToClose() override;

 private:
  std::string domainDescriptor_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};
  ClosingEmitter closingEmitter_;

  // An identifier for the context, composed of the identifier for the context,
  // combined with the channel's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  // Sequence numbers for the channels created by this context, used to create
  // their identifiers based off this context's identifier. They will only be
  // used for logging and debugging.
  std::atomic<uint64_t> channelCounter_{0};

  // InfiniBand stuff
  bool foundIbvLib_{false};
  IbvLib ibvLib_;
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
