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
    fi_addr_t peer_addr,
    void* context) {
  pendingSends_.emplace_back(EfaEvent(new
  fi_msg_tagged{
      .msg_iov = new iovec{.iov_base = buffer, .iov_len = size},
      .iov_count = 1,
      .addr = peer_addr,
      .tag = tag,
      .context = context}));
  postPendingRecvs();
}

void Reactor::postRecv(
    void* buffer,
    size_t size,
    uint64_t tag,
    fi_addr_t dest_addr,
    uint64_t ignore,
    void* context) {
  pendingRecvs_.emplace_back(EfaEvent(new
  fi_msg_tagged{
      .msg_iov = new iovec{.iov_base = buffer, .iov_len = size},
      .iov_count = 1,
      .addr = dest_addr,
      .tag = tag,
      .ignore = ignore,
      .context = context}));
  postPendingRecvs();
}

int Reactor::postPendingSends() {
  while (!pendingSends_.empty()) {
    fi_msg_tagged* sevent = pendingSends_.front().get();
    int ret = efaLib_.fi_tsendmsg_op(ep_.get(), sevent, 0);
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
  fi_addr_t peer_addr;
  int ret =
      efaLib_.fi_av_insert_op(av_.get(), addr.name, 1, &peer_addr, 0, nullptr);
  TP_THROW_ASSERT_IF(ret != 1) << "Unable to add address to endpoint";
  TP_CHECK_EFA_RET(ret, "Unable to add address to endpoint");
  return peer_addr;
}

void Reactor::removePeerAddr(fi_addr_t faddr) {
  int ret = efaLib_.fi_av_remove_op(av_.get(), &faddr, 1, 0);
  TP_CHECK_EFA_RET(ret, "Unable to remove address from endpoint");
};

int Reactor::postPendingRecvs() {
  while (!pendingRecvs_.empty()) {
    fi_msg_tagged* revent = pendingRecvs_.front().get();
    int ret = efaLib_.fi_trecvmsg_op(ep_.get(), revent, 0);
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
  std::array<struct fi_cq_tagged_entry, kNumPolledWorkCompletions> cq_entries;
  std::array<fi_addr_t, kNumPolledWorkCompletions> src_addrs;

  postPendingSends();
  postPendingRecvs();
  int rv = efaLib_.fi_cq_readfrom_op(
      cq_.get(), cq_entries.data(), cq_entries.size(), src_addrs.data());
  if (rv == 0 || rv == -FI_EAGAIN) {
    return false;
  } else {
    TP_CHECK_EFA_RET(rv, "Completion queue poll error.");
  }

  int numRecvs = 0;
  int numWrites = 0;
  int numAcks = 0;
  for (int cqIdx = 0; cqIdx < rv; cqIdx++) {
    struct fi_cq_tagged_entry& cq = cq_entries[cqIdx];
    fi_addr_t& src_addr = src_addrs[cqIdx];
    uint32_t msg_idx = static_cast<uint32_t>(cq.tag);
    if (cq.flags & FI_SEND) {
      // Send event
      if (cq.tag & kLength) {
        // Send size finished, check whether it's zero sized message
        auto* operation_ptr = static_cast<EFAWriteOperation*>(cq.op_context);
        if (operation_ptr->getLength() == 0) {
          operation_ptr->setCompleted();
          efaEventHandler_[operation_ptr->getPeerAddr()]->onWriteCompleted();
        }
      } else if (cq.tag & kPayload) {
        auto* operation_ptr = static_cast<EFAWriteOperation*>(cq.op_context);
        operation_ptr->setCompleted();
        efaEventHandler_[operation_ptr->getPeerAddr()]->onWriteCompleted();
      }
    } else if (cq.flags & FI_RECV) {
      // Receive event
      if (cq.tag & kLength) {
        // Received length information
        auto* operation_ptr = static_cast<EFAReadOperation*>(cq.op_context);
        if (operation_ptr->getReadLength() == 0) {
          operation_ptr->setCompleted();
          efaEventHandler_[src_addr]->onReadCompleted();
        } else {
          // operation_ptr->mode_ = EFAReadOperation::Mode::READ_PAYLOAD;
          operation_ptr->allocFromLoop();
          postRecv(
              operation_ptr->getBufferPtr(),
              operation_ptr->getReadLength(),
              kPayload | msg_idx,
              src_addr,
              0, // Exact match of tag
              operation_ptr);
          operation_ptr->setWaitToCompleted();
        }
      } else if (cq.tag & kPayload) {
        // Received payload
        auto* operation_ptr = static_cast<EFAReadOperation*>(cq.op_context);
        operation_ptr->setCompleted();
        efaEventHandler_[src_addr]->onReadCompleted();
      }
    }
  }

  return true;
}

bool Reactor::readyToClose() {
  return efaEventHandler_.size() == 0;
}

void Reactor::registerHandler(
    fi_addr_t peer_addr,
    std::shared_ptr<efaEventHandler> eventHandler) {
  efaEventHandler_.emplace(peer_addr, std::move(eventHandler));
}

void Reactor::unregisterHandler(fi_addr_t peer_addr) {
  efaEventHandler_.erase(peer_addr);
}

} // namespace efa
} // namespace transport
} // namespace tensorpipe
