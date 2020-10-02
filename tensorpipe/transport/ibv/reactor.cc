/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/reactor.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/ibv/constants.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

Reactor::Reactor() {
  IbvDeviceList deviceList;
  TP_THROW_ASSERT_IF(deviceList.size() == 0);
  ctx_ = IbvContext(deviceList[0]);
  pd_ = IbvProtectionDomain(ctx_);
  cq_ = IbvCompletionQueue(
      ctx_,
      kCompletionQueueSize,
      /*cq_context=*/nullptr,
      /*channel=*/nullptr,
      /*comp_vector=*/0);

  struct ibv_srq_init_attr srqInitAttr;
  std::memset(&srqInitAttr, 0, sizeof(srqInitAttr));
  srqInitAttr.attr.max_wr = kNumPendingRecvReqs;
  srq_ = IbvSharedReceiveQueue(pd_, srqInitAttr);

  addr_ = makeIbvAddress(ctx_, kPortNum, kGlobalIdentifierIndex);

  postRecvRequestsOnSRQ_(kNumPendingRecvReqs);

  thread_ = std::thread(&Reactor::run, this);
}

void Reactor::postRecvRequestsOnSRQ_(int num) {
  while (num > 0) {
    struct ibv_recv_wr* badRecvWr = nullptr;
    std::array<struct ibv_recv_wr, kNumPolledWorkCompletions> wrs;
    std::memset(wrs.data(), 0, sizeof(wrs));
    for (int i = 0; i < std::min(num, kNumPolledWorkCompletions) - 1; i++) {
      wrs[i].next = &wrs[i + 1];
    }
    int rv = ibv_post_srq_recv(srq_.ptr(), wrs.data(), &badRecvWr);
    TP_THROW_SYSTEM_IF(rv != 0, errno);
    TP_THROW_ASSERT_IF(badRecvWr != nullptr);
    num -= std::min(num, kNumPolledWorkCompletions);
  }
}

void Reactor::close() {
  if (!closed_.exchange(true)) {
    // No need to wake up the reactor, since it is busy-waiting.
  }
}

void Reactor::join() {
  close();

  if (!joined_.exchange(true)) {
    thread_.join();
  }
}

Reactor::~Reactor() {
  join();
}

void Reactor::run() {
  setThreadName("TP_IBV_reactor");

  // Stop when another thread has asked the reactor the close and when
  // all functions have been removed.
  while (!closed_ || queuePairEventHandler_.size() > 0) {
    std::array<struct ibv_wc, kNumPolledWorkCompletions> wcs;
    auto rv = ibv_poll_cq(cq_.ptr(), wcs.size(), wcs.data());

    if (rv == 0) {
      if (deferredFunctionCount_ > 0) {
        decltype(deferredFunctionList_) fns;

        {
          std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
          std::swap(fns, deferredFunctionList_);
        }

        deferredFunctionCount_ -= fns.size();

        for (auto& fn : fns) {
          fn();
        }
      } else {
        std::this_thread::yield();
      }
      continue;
    }
    TP_THROW_SYSTEM_IF(rv < 0, errno);

    int numRecvs = 0;
    int numWrites = 0;
    int numAcks = 0;
    for (int wcIdx = 0; wcIdx < rv; wcIdx++) {
      struct ibv_wc& wc = wcs[wcIdx];

      auto iter = queuePairEventHandler_.find(wc.qp_num);
      TP_THROW_ASSERT_IF(iter == queuePairEventHandler_.end())
          << "Got work completion for unknown queue pair " << wc.qp_num;

      if (wc.status != IBV_WC_SUCCESS) {
        iter->second->onError(wc.status);
        continue;
      }

      TP_THROW_ASSERT_IF(!(wc.wc_flags & IBV_WC_WITH_IMM));
      switch (wc.opcode) {
        case IBV_WC_RECV_RDMA_WITH_IMM:
          iter->second->onRemoteProducedData(wc.imm_data);
          numRecvs++;
          break;
        case IBV_WC_RECV:
          iter->second->onRemoteConsumedData(wc.imm_data);
          numRecvs++;
          break;
        case IBV_WC_RDMA_WRITE:
          iter->second->onWriteCompleted();
          numWrites++;
          break;
        case IBV_WC_SEND:
          iter->second->onAckCompleted();
          numAcks++;
          break;
        default:
          TP_THROW_ASSERT() << "Unknown opcode: " << wc.opcode;
      }
    }

    postRecvRequestsOnSRQ_(numRecvs);

    numAvailableWrites_ += numWrites;
    while (!pendingQpWrites_.empty() && numAvailableWrites_ > 0) {
      postWrite(
          std::get<0>(pendingQpWrites_.front()),
          std::get<1>(pendingQpWrites_.front()));
      pendingQpWrites_.pop_front();
    }

    numAvailableAcks_ += numAcks;
    while (!pendingQpAcks_.empty() && numAvailableAcks_ > 0) {
      postAck(
          std::get<0>(pendingQpAcks_.front()),
          std::get<1>(pendingQpAcks_.front()));
      pendingQpAcks_.pop_front();
    }
  }

  // The loop is winding down and "handing over" control to the on demand loop.
  // But it can only do so safely once there are no pending deferred functions,
  // as otherwise those may risk never being executed.
  while (true) {
    decltype(deferredFunctionList_) fns;

    {
      std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
      if (deferredFunctionList_.empty()) {
        isThreadConsumingDeferredFunctions_ = false;
        break;
      }
      std::swap(fns, deferredFunctionList_);
    }

    for (auto& fn : fns) {
      fn();
    }
  }
}

void Reactor::registerQp(
    uint32_t qpn,
    std::shared_ptr<IbvEventHandler> eventHandler) {
  queuePairEventHandler_.emplace(qpn, std::move(eventHandler));
}

void Reactor::unregisterQp(uint32_t qpn) {
  queuePairEventHandler_.erase(qpn);
}

void Reactor::deferToLoop(TDeferredFunction fn) {
  {
    std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
    if (likely(isThreadConsumingDeferredFunctions_)) {
      deferredFunctionList_.push_back(std::move(fn));
      ++deferredFunctionCount_;
      // No need to wake up the reactor, since it is busy-waiting.
      return;
    }
  }
  // Must call it without holding the lock, as it could cause a reentrant call.
  onDemandLoop_.deferToLoop(std::move(fn));
}

void Reactor::postWrite(IbvQueuePair& qp, struct ibv_send_wr& wr) {
  if (numAvailableWrites_ > 0) {
    struct ibv_send_wr* badWr = nullptr;
    TP_CHECK_IBV_INT(ibv_post_send(qp.ptr(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableWrites_--;
  } else {
    pendingQpWrites_.emplace_back(qp, wr);
  }
}

void Reactor::postAck(IbvQueuePair& qp, struct ibv_send_wr& wr) {
  if (numAvailableAcks_ > 0) {
    struct ibv_send_wr* badWr = nullptr;
    TP_CHECK_IBV_INT(ibv_post_send(qp.ptr(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableAcks_--;
  } else {
    pendingQpAcks_.emplace_back(qp, wr);
  }
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
