/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/efa/reactor.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/common/efa_read_write_ops.h>
#include <tensorpipe/transport/efa/constants.h>

namespace tensorpipe {
namespace transport {
namespace efa {

Reactor::Reactor() {
  postRecvRequests(kNumPendingRecvReqs);
  startThread("TP_efa_reactor");
}

void Reactor::postSend(
    void* buffer,
    size_t size,
    uint64_t tag,
    fi_addr_t peer_addr,
    void* context) {
  // First try send all messages in pending queue
  while (!pendingSends_.empty()) {
    EFASendEvent sevent = pendingSends_.front();
    int ret =
        this->endpoint->PushSendEvent(sevent.buffer, sevent.size, sevent.tag, sevent.peer_addr, sevent.context);
    if (ret == 0) {
      // Send successfully, pop out events
      pendingSends_.pop_front();
    } else if (ret == -FI_EAGAIN) {
      // Event queue is full now, push the event into pending queue and return
      pendingSends_.push_back({buffer, size, tag, peer_addr, context});
      return;
    } else if (ret < 0) {
      // Unknown failure, raise exception
      TP_CHECK_EFA_RET(ret, "Unable to do fi_tsend message");
    }
  }

  // No pending events, send out directly
  int ret =
      this->endpoint->PushSendEvent(buffer, size, tag, peer_addr, context);
  if (ret == 0) {
    // Send successfully
    return;
  } else if (ret == -FI_EAGAIN) {
    // Event queue is full now, push the event into pending queue and return
    pendingSends_.push_back({buffer, size, tag, peer_addr, context});
    return;
  } else if (ret < 0) {
    TP_CHECK_EFA_RET(ret, "Unable to do fi_tsend message");
  }
}

int Reactor::postPendingSends() {
  while (!pendingSends_.empty()) {
    EFASendEvent sevent = pendingSends_.front();
    int ret = this->endpoint->PushSendEvent(
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
    return 0;
  }
}

int Reactor::postPendingRecvs() {
  while (!pendingRecvs_.empty()) {
    EFARecvEvent revent = pendingRecvs_.front();
    int ret = this->endpoint->PushRecvEvent(
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
      TP_CHECK_EFA_RET(ret, "Unable to do fi_tsend message");
    }
  }
  return 0;
}

void Reactor::postRecv(
    void* buffer,
    size_t size,
    uint64_t tag,
    fi_addr_t dest_addr,
    uint64_t ignore,
    void* context) {
  // First try send all messages in pending queue
  int pendingRecvNum = postPendingRecvs();
  if (pendingRecvNum == 0){
    // No pending events, send out directly
    int ret =
        this->endpoint->PushRecvEvent(buffer, size, tag, dest_addr, ignore, context);
    if (ret == 0) {
      // Send successfully
      return;
    } else if (ret == -FI_EAGAIN) {
      // Event queue is full now, push the event into pending queue and return
      pendingRecvs_.push_back({buffer, size, tag, dest_addr, ignore, context});
      return;
    } else if (ret < 0) {
      TP_CHECK_EFA_RET(ret, "Unable to do fi_tsend message");
    }
  } else {
    pendingRecvs_.push_back({buffer, size, tag, dest_addr, ignore, context});
    return;
  }

}

// void Reactor::postRecvRequests(int num) {
//   while (num > 0) {
//     efaLib::recv_wr* badRecvWr = nullptr;
//     std::array<efaLib::recv_wr, kNumPolledWorkCompletions> wrs;
//     std::memset(wrs.data(), 0, sizeof(wrs));
//     for (int i = 0; i < std::min(num, kNumPolledWorkCompletions) - 1; i++) {
//       wrs[i].next = &wrs[i + 1];
//     }
//     int rv = getefaLib().post_srq_recv(srq_.get(), wrs.data(), &badRecvWr);
//     TP_THROW_SYSTEM_IF(rv != 0, errno);
//     TP_THROW_ASSERT_IF(badRecvWr != nullptr);
//     num -= std::min(num, kNumPolledWorkCompletions);
//   }
// }

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

// void Reactor::postRecvRequests(int num){
//   uint64_t size_buffer;
//   int ret = endpoint->PushRecvEvent(&size_buffer, sizeof(uint64_t), kLength,
//   FI_ADDR_UNSPEC);

// }

bool Reactor::pollOnce() {
  std::array<struct fi_cq_tagged_entry, kNumPolledWorkCompletions> cq_entries;
  std::array<fi_addr_t, kNumPolledWorkCompletions> src_addrs;
  auto rv =
      endpoint->PollCQ(cq_entries.data(), src_addrs.data(), cq_entries.size());

  if (rv == 0) {
    return false;
  }
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  int numRecvs = 0;
  int numWrites = 0;
  int numAcks = 0;
  for (int cqIdx = 0; cqIdx < rv; cqIdx++) {
    struct fi_cq_tagged_entry& cq = cq_entries[cqIdx];
    fi_addr_t& src_addr = src_addrs[cqIdx];
    uint32_t msg_idx = static_cast<uint32_t>(cq.tag);
    if (cq.flags && FI_SEND) {
      // Send event
      if (cq.flags && kLength) {
        // Send size finished, check whether it's zero sized message
        auto* operation_ptr = static_cast<EFAWriteOperation*>(cq.op_context);
        if (operation_ptr->length_ == 0){
           operation_ptr->mode_ = EFAWriteOperation::Mode::COMPLETE;
           operation_ptr->callbackFromLoop(Error::kSuccess);
        }
      } else if (cq.flags && kPayload) {
        auto* operation_ptr = static_cast<EFAWriteOperation*>(cq.op_context);
        operation_ptr->mode_ = EFAWriteOperation::Mode::COMPLETE;
        operation_ptr->callbackFromLoop(Error::kSuccess);
      }
    } else if (cq.flags && FI_RECV) {
      // Receive event
      // auto iter = efaEventHandler_.find(src_addr);
      if (cq.tag && kLength) {
        // Received length information
        auto* operation_ptr = static_cast<EFAReadOperation*>(cq.op_context);
        operation_ptr->mode_ = EFAReadOperation::Mode::READ_PAYLOAD;
        operation_ptr->allocFromLoop();
        //  postRecv()
        //  void* buffer = operation_ptr->perpareBuffer();
        postRecv(
            operation_ptr->ptr_,
            operation_ptr->readLength_,
            kPayload | msg_idx,
            src_addr,
            0, // Exact match of tag
            operation_ptr);
        //  iter->second->onRecvLength(msg_idx);
      } else if (cq.tag && kPayload) {
        // Received payload
        auto* operation_ptr = static_cast<EFAReadOperation*>(cq.op_context);
        operation_ptr->mode_ = EFAReadOperation::Mode::COMPLETE;
        operation_ptr->callbackFromLoop(Error::kSuccess);
      }
    }
    // auto iter = queuePairEventHandler_.find(wc.qp_num);
    // TP_THROW_ASSERT_IF(iter == queuePairEventHandler_.end())
    //     << "Got work completion for unknown queue pair " << wc.qp_num;

    // if (wc.status != efaLib::WC_SUCCESS) {
    //   iter->second->onError(wc.status, wc.wr_id);
    //   continue;
    // }
  }

  return true;
}

bool Reactor::readyToClose() {
  return true;
  // return queuePairEventHandler_.size() == 0;
}

// void Reactor::registerQp(
//     uint32_t qpn,
//     std::shared_ptr<efaEventHandler> eventHandler) {
//   queuePairEventHandler_.emplace(qpn, std::move(eventHandler));
// }

} // namespace efa
} // namespace transport
} // namespace tensorpipe
