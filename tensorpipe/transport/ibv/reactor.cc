/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

Reactor::Reactor(IbvLib ibvLib, IbvDeviceList deviceList)
    : ibvLib_(std::move(ibvLib)) {
  TP_DCHECK_GE(deviceList.size(), 1);
  ctx_ = createIbvContext(getIbvLib(), deviceList[0]);
  pd_ = createIbvProtectionDomain(getIbvLib(), ctx_);
  cq_ = createIbvCompletionQueue(
      getIbvLib(),
      ctx_,
      kCompletionQueueSize,
      /*cq_context=*/nullptr,
      /*channel=*/nullptr,
      /*comp_vector=*/0);

  IbvLib::srq_init_attr srqInitAttr;
  std::memset(&srqInitAttr, 0, sizeof(srqInitAttr));
  srqInitAttr.attr.max_wr = kNumPendingRecvReqs;
  srq_ = createIbvSharedReceiveQueue(getIbvLib(), pd_, srqInitAttr);

  addr_ = makeIbvAddress(getIbvLib(), ctx_, kPortNum, kGlobalIdentifierIndex);

  postRecvRequestsOnSRQ(kNumPendingRecvReqs);

  startThread("TP_IBV_reactor");
}

void Reactor::postRecvRequestsOnSRQ(int num) {
  while (num > 0) {
    IbvLib::recv_wr* badRecvWr = nullptr;
    std::array<IbvLib::recv_wr, kNumPolledWorkCompletions> wrs;
    std::memset(wrs.data(), 0, sizeof(wrs));
    for (int i = 0; i < std::min(num, kNumPolledWorkCompletions) - 1; i++) {
      wrs[i].next = &wrs[i + 1];
    }
    int rv = getIbvLib().post_srq_recv(srq_.get(), wrs.data(), &badRecvWr);
    TP_THROW_SYSTEM_IF(rv != 0, errno);
    TP_THROW_ASSERT_IF(badRecvWr != nullptr);
    num -= std::min(num, kNumPolledWorkCompletions);
  }
}

void Reactor::setId(std::string id) {
  id_ = std::move(id);
}

void Reactor::close() {
  if (!closed_.exchange(true)) {
    stopBusyPolling();
  }
}

void Reactor::join() {
  close();

  if (!joined_.exchange(true)) {
    joinThread();
  }
}

Reactor::~Reactor() {
  join();
}

bool Reactor::pollOnce() {
  std::array<IbvLib::wc, kNumPolledWorkCompletions> wcs;
  auto rv = getIbvLib().poll_cq(cq_.get(), wcs.size(), wcs.data());

  if (rv == 0) {
    return false;
  }
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  int numRecvs = 0;
  int numWrites = 0;
  int numAcks = 0;
  for (int wcIdx = 0; wcIdx < rv; wcIdx++) {
    IbvLib::wc& wc = wcs[wcIdx];

    TP_VLOG(9) << "Transport context " << id_
               << " got work completion for request " << wc.wr_id << " for QP "
               << wc.qp_num << " with status "
               << getIbvLib().wc_status_str(wc.status) << " and opcode "
               << ibvWorkCompletionOpcodeToStr(wc.opcode)
               << " (byte length: " << wc.byte_len
               << ", immediate data: " << wc.imm_data << ")";

    auto iter = queuePairEventHandler_.find(wc.qp_num);
    TP_THROW_ASSERT_IF(iter == queuePairEventHandler_.end())
        << "Got work completion for unknown queue pair " << wc.qp_num;

    if (wc.status != IbvLib::WC_SUCCESS) {
      iter->second->onError(wc.status, wc.wr_id);
      continue;
    }

    switch (wc.opcode) {
      case IbvLib::WC_RECV_RDMA_WITH_IMM:
        TP_THROW_ASSERT_IF(!(wc.wc_flags & IbvLib::WC_WITH_IMM));
        iter->second->onRemoteProducedData(wc.imm_data);
        numRecvs++;
        break;
      case IbvLib::WC_RECV:
        TP_THROW_ASSERT_IF(!(wc.wc_flags & IbvLib::WC_WITH_IMM));
        iter->second->onRemoteConsumedData(wc.imm_data);
        numRecvs++;
        break;
      case IbvLib::WC_RDMA_WRITE:
        iter->second->onWriteCompleted();
        numWrites++;
        break;
      case IbvLib::WC_SEND:
        iter->second->onAckCompleted();
        numAcks++;
        break;
      default:
        TP_THROW_ASSERT() << "Unknown opcode: " << wc.opcode;
    }
  }

  postRecvRequestsOnSRQ(numRecvs);

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

  return true;
}

bool Reactor::readyToClose() {
  return queuePairEventHandler_.size() == 0;
}

void Reactor::registerQp(
    uint32_t qpn,
    std::shared_ptr<IbvEventHandler> eventHandler) {
  queuePairEventHandler_.emplace(qpn, std::move(eventHandler));
}

void Reactor::unregisterQp(uint32_t qpn) {
  queuePairEventHandler_.erase(qpn);
}

void Reactor::postWrite(IbvQueuePair& qp, WriteInfo info) {
  if (numAvailableWrites_ > 0) {
    IbvLib::sge list;
    list.addr = reinterpret_cast<uint64_t>(info.addr);
    list.length = info.length;
    list.lkey = info.lkey;

    IbvLib::send_wr wr;
    std::memset(&wr, 0, sizeof(wr));
    wr.wr_id = kWriteRequestId;
    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode = IbvLib::WR_RDMA_WRITE_WITH_IMM;
    wr.imm_data = info.length;
    wr.wr.rdma.remote_addr = info.remoteAddr;
    wr.wr.rdma.rkey = info.rkey;

    IbvLib::send_wr* badWr = nullptr;
    TP_VLOG(9) << "Transport context " << id_ << " posting RDMA write for QP "
               << qp->qp_num;
    TP_CHECK_IBV_INT(getIbvLib().post_send(qp.get(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableWrites_--;
  } else {
    TP_VLOG(9) << "Transport context " << id_
               << " queueing up RDMA write for QP " << qp->qp_num;
    pendingQpWrites_.emplace_back(qp, info);
  }
}

void Reactor::postAck(IbvQueuePair& qp, AckInfo info) {
  if (numAvailableAcks_ > 0) {
    IbvLib::send_wr wr;
    std::memset(&wr, 0, sizeof(wr));
    wr.wr_id = kAckRequestId;
    wr.opcode = IbvLib::WR_SEND_WITH_IMM;
    wr.imm_data = info.length;

    IbvLib::send_wr* badWr = nullptr;
    TP_VLOG(9) << "Transport context " << id_ << " posting send for QP "
               << qp->qp_num;
    TP_CHECK_IBV_INT(getIbvLib().post_send(qp.get(), &wr, &badWr));
    TP_THROW_ASSERT_IF(badWr != nullptr);
    numAvailableAcks_--;
  } else {
    TP_VLOG(9) << "Transport context " << id_ << " queueing send for QP "
               << qp->qp_num;
    pendingQpAcks_.emplace_back(qp, info);
  }
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
