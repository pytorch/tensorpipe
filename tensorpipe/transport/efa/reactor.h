/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <list>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <tensorpipe/common/busy_polling_loop.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/fd.h>
#include <tensorpipe/common/efa.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/efa/constants.h>

namespace tensorpipe {
namespace transport {
namespace efa {

// class efaEventHandler {
//  public:
//   virtual void onRecvLength(uint32_t msg_idx) = 0;

//   virtual void onSendData(uint32_t msg_idx) = 0;

//   virtual void onSendCompleted(uint32_t msg_idx) = 0;

//   virtual void onRecvCompleted(uint32_t msg_idx) = 0;

//   virtual void onError(int errno) = 0;

//   virtual ~efaEventHandler() = default;
// };

enum efaTag: uint64_t{
  kLength = 1ULL << 32,
  kPayload = 1ULL << 33,
};

// Reactor loop.
//
// Companion class to the event loop in `loop.h` that executes
// functions on triggers. The triggers are posted to a shared memory
// ring buffer, so this can be done by other processes on the same
// machine. It uses extra data in the ring buffer header to store a
// mutex and condition variable to avoid a busy loop.
//
class Reactor final : public BusyPollingLoop {
 public:
  // Reactor(efaLib efaLib, efaDeviceList deviceList);
  Reactor();

  // const efaLib& getefaLib() {
  //   return efaLib_;
  // }

  // efaProtectionDomain& getefaPd() {
  //   return pd_;
  // }

  // efaCompletionQueue& getefaCq() {
  //   return cq_;
  // }

  // efaSharedReceiveQueue& getefaSrq() {
  //   return srq_;
  // }

  // const efaAddress& getefaAddress() {
  //   return addr_;
  // }

  // void registerQp(uint32_t qpn, std::shared_ptr<efaEventHandler> eventHandler);

  // void unregisterQp(uint32_t qpn);

  void postSend(void* buffer, size_t size, uint64_t tag, fi_addr_t peer_addr, void* context);

  void postRecv(void* buffer, size_t size, uint64_t tag, fi_addr_t peer_addr, uint64_t ignore, void* context);
  // void postAck(efaQueuePair& qp, efaLib::send_wr& wr);

  void setId(std::string id);

  void close();

  void join();

  ~Reactor();

 protected:
  bool pollOnce() override;

  bool readyToClose() override;

  struct EFASendEvent{
    void* buffer;
    size_t size;
    uint64_t tag;
    fi_addr_t peer_addr;
    void* context;
  };

  struct EFARecvEvent{
    void* buffer;
    size_t size;
    uint64_t tag;
    fi_addr_t peer_addr;
    uint64_t ignore;
    void* context;
  };
 private:
  // InfiniBand stuff
  // const efaLib efaLib_;
  // efaContext ctx_;
  // efaProtectionDomain pd_;
  // efaCompletionQueue cq_;
  // efaSharedReceiveQueue srq_;
  // efaAddress addr_;
  int postPendingRecvs();
  int postPendingSends();

  std::shared_ptr<FabricEndpoint> endpoint;

  // void postRecvRequests(int num);

  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  std::array<uint64_t, kNumPendingRecvReqs> size_buffer;

  // An identifier for the context, composed of the identifier for the context,
  // combined with the transport's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  // The registered event handlers for each queue pair.
  // std::unordered_map<fi_addr_t, std::shared_ptr<efaEventHandler>>
  //     efaEventHandler_;

  // uint32_t numAvailableWrites_{kNumPendingWriteReqs};
  // uint32_t numAvailableAcks_{kNumPendingAckReqs};
  std::deque<EFASendEvent> pendingSends_;
  std::deque<EFARecvEvent> pendingRecvs_;
  // std::deque<std::tuple<efaQueuePair&, efaLib::send_wr>> pendingQpAcks_;
};

} // namespace efa
} // namespace transport
} // namespace tensorpipe
