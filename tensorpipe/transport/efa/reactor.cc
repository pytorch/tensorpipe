/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/efa/reactor.h>

#include <tensorpipe/common/efa_read_write_ops.h>
#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/efa/connection_impl.h>
#include <tensorpipe/transport/efa/constants.h>

namespace tensorpipe {
namespace transport {
namespace efa {

Reactor::Reactor(EfaLib efaLib, EfaDeviceList efaDeviceList) {
  efaLib_ = std::move(efaLib);
  // AWS p4d instances may have multiple EFAs. Only use device 0 for now
  EfaLib::device* device = &efaDeviceList[0];
  fabric_ = createEfaFabric(efaLib, device);
  domain_ = createEfaDomain(efaLib, fabric_, device);
  ep_ = createEfaEndpoint(efaLib, domain_, device);
  av_ = createEfaAdressVector(efaLib, domain_);
  cq_ = createEfaCompletionQueue(efaLib, domain_, device);
  addr_ = enableEndpoint(efaLib, ep_, av_, cq_);
  startThread("TP_efa_reactor");
}

void Reactor::postSend(
    void* buffer,
    size_t size,
    uint64_t tag,
    fi_addr_t peerAddr,
    void* context) {
  pendingSends_.emplace_back(EfaEvent((new fi_msg_tagged{
      /* msg_iov */ new iovec{.iov_base = buffer, .iov_len = size},
      /* desc */ 0,
      /* iov_count */ 1,
      /* peer addr */ peerAddr,
      /* tag */ tag,
      /* ignore */ 0,
      /* context */ context,
      /* data */ 0})));
  postPendingRecvs();
}

void Reactor::postRecv(
    void* buffer,
    size_t size,
    uint64_t tag,
    fi_addr_t peerAddr,
    uint64_t ignore,
    void* context) {
  pendingRecvs_.emplace_back(EfaEvent(new fi_msg_tagged{
      /* msg_iov */ new iovec{.iov_base = buffer, .iov_len = size},
      /* desc */ 0,
      /* iov_count */ 1,
      /* peer addr */ peerAddr,
      /* tag */ tag,
      /* ignore */ ignore,
      /* context */ context,
      /* data */ 0}));
  postPendingRecvs();
}

int Reactor::postPendingSends() {
  while (!pendingSends_.empty()) {
    fi_msg_tagged* sevent = pendingSends_.front().get();
    int ret = fi_tsendmsg(ep_.get(), sevent, 0);
    if (ret == 0) {
      // Send successfully, pop out events
      pendingSends_.pop_front();
    } else if (ret == -FI_EAGAIN) {
      return pendingSends_.size();
    } else if (ret < 0) {
      // Unknown failure, raise exception
      TP_CHECK_EFA_RET(ret, "Unable to do fi_tsend message");
    }
  }

  return 0;
}

fi_addr_t Reactor::addPeerAddr(EfaAddress& addr) {
  fi_addr_t peerAddr;
  int ret = fi_av_insert(av_.get(), addr.name, 1, &peerAddr, 0, nullptr);
  TP_THROW_ASSERT_IF(ret != 1) << "Unable to add address to endpoint";
  TP_CHECK_EFA_RET(ret, "Unable to add address to endpoint");
  efaAddrSet_.emplace(peerAddr);
  return peerAddr;
}

void Reactor::removePeerAddr(fi_addr_t faddr) {
  int ret = fi_av_remove(av_.get(), &faddr, 1, 0);
  TP_CHECK_EFA_RET(ret, "Unable to remove address from endpoint");
  efaAddrSet_.erase(faddr);
};

int Reactor::postPendingRecvs() {
  while (!pendingRecvs_.empty()) {
    fi_msg_tagged* revent = pendingRecvs_.front().get();
    int ret = fi_trecvmsg(ep_.get(), revent, 0);
    if (ret == 0) {
      // Send successfully, pop out events
      pendingRecvs_.pop_front();
    } else if (ret == -FI_EAGAIN) {
      return pendingRecvs_.size();
    } else if (ret < 0) {
      // Unknown failure, raise exception
      TP_CHECK_EFA_RET(ret, "Unable to do fi_trecv message");
    }
  }
  return 0;
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
  std::array<struct fi_cq_tagged_entry, kNumPolledWorkCompletions> cqEntries;
  std::array<fi_addr_t, kNumPolledWorkCompletions> srcAddrs;

  postPendingSends();
  postPendingRecvs();
  int rv = fi_cq_readfrom(
      cq_.get(), cqEntries.data(), cqEntries.size(), srcAddrs.data());
  if (rv == 0 || rv == -FI_EAGAIN) {
    return false;
  } else {
    TP_CHECK_EFA_RET(rv, "Completion queue poll error.");
  }

  int numRecvs = 0;
  int numWrites = 0;
  int numAcks = 0;
  for (int cqIdx = 0; cqIdx < rv; cqIdx++) {
    struct fi_cq_tagged_entry& cq = cqEntries[cqIdx];
    fi_addr_t& srcAddr = srcAddrs[cqIdx];
    uint32_t msgIdx = static_cast<uint32_t>(cq.tag);
    if (cq.flags & FI_SEND) {
      // Send event
      if (cq.tag & kLength) {
        // Send size finished, check whether it's zero sized message
        auto* operationPtr = static_cast<EFAWriteOperation*>(cq.op_context);
        if (operationPtr->getLength() == 0) {
          operationPtr->setCompleted();
          reinterpret_cast<ConnectionImpl*>(operationPtr->getOpContext())
              ->onWriteCompleted();
        }
      } else if (cq.tag & kPayload) {
        auto* operationPtr = static_cast<EFAWriteOperation*>(cq.op_context);
        operationPtr->setCompleted();
        reinterpret_cast<ConnectionImpl*>(operationPtr->getOpContext())
            ->onWriteCompleted();
      }
    } else if (cq.flags & FI_RECV) {
      // Receive event
      if (cq.tag & kLength) {
        // Received length information
        auto* operationPtr = static_cast<EFAReadOperation*>(cq.op_context);
        if (operationPtr->getReadLength() == 0) {
          operationPtr->setCompleted();
          reinterpret_cast<ConnectionImpl*>(operationPtr->getOpContext())
              ->onReadCompleted();
        } else {
          // operation_ptr->mode_ = EFAReadOperation::Mode::READ_PAYLOAD;
          operationPtr->allocFromLoop();
          postRecv(
              operationPtr->getBufferPtr(),
              operationPtr->getReadLength(),
              kPayload | msgIdx,
              srcAddr,
              0, // Exact match of tag
              operationPtr);
          operationPtr->setWaitToCompleted();
        }
      } else if (cq.tag & kPayload) {
        // Received payload
        auto* operationPtr = static_cast<EFAReadOperation*>(cq.op_context);
        operationPtr->setCompleted();
        reinterpret_cast<ConnectionImpl*>(operationPtr->getOpContext())
            ->onReadCompleted();
      }
    }
  }

  return true;
}

bool Reactor::readyToClose() {
  return efaAddrSet_.size() == 0;
}

} // namespace efa
} // namespace transport
} // namespace tensorpipe
