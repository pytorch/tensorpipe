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

Reactor::Reactor() {
  // postRecvRequests(kNumPendingRecvReqs);
  endpoint = std::make_shared<FabricEndpoint>();
  startThread("TP_efa_reactor");
}

void Reactor::postSend(
    void* buffer,
    size_t size,
    uint64_t tag,
    fi_addr_t peer_addr,
    void* context) {
  pendingSends_.push_back({buffer, size, tag, peer_addr, context});
  postPendingRecvs();
}

void Reactor::postRecv(
    void* buffer,
    size_t size,
    uint64_t tag,
    fi_addr_t dest_addr,
    uint64_t ignore,
    void* context) {
  // First try send all messages in pending queue
  pendingRecvs_.push_back({buffer, size, tag, dest_addr, ignore, context});
  postPendingRecvs();
}

int Reactor::postPendingSends() {
  // TP_LOG_WARNING() << "Post send event";
  while (!pendingSends_.empty()) {
    EFASendEvent sevent = pendingSends_.front();
    int ret = this->endpoint->post_send(
        sevent.buffer,
        sevent.size,
        sevent.tag,
        sevent.peer_addr,
        sevent.context); // ignore low 32 bits on tag matching
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

int Reactor::postPendingRecvs() {
  while (!pendingRecvs_.empty()) {
    EFARecvEvent revent = pendingRecvs_.front();
    int ret = this->endpoint->post_recv(
        revent.buffer,
        revent.size,
        revent.tag,
        revent.peer_addr,
        revent.ignore,
        revent.context); // ignore low 32 bits on tag matching
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

  auto rv =
      endpoint->poll_cq(cq_entries.data(), src_addrs.data(), cq_entries.size());

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

void Reactor::registerHandler(fi_addr_t peer_addr, std::shared_ptr<efaEventHandler> eventHandler) {
  efaEventHandler_.emplace(peer_addr, std::move(eventHandler));
}

void Reactor::unregisterHandler(fi_addr_t peer_addr){
  efaEventHandler_.erase(peer_addr);
}

} // namespace efa
} // namespace transport
} // namespace tensorpipe
