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

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/fd.h>
#include <tensorpipe/common/ibv.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/ibv/constants.h>
#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

class IbvEventHandler {
 public:
  virtual void onRemoteProducedData(uint32_t length) = 0;

  virtual void onRemoteConsumedData(uint32_t length) = 0;

  virtual void onWriteCompleted() = 0;

  virtual void onAckCompleted() = 0;

  virtual void onError(IbvLib::wc_status status, uint64_t wr_id) = 0;

  virtual ~IbvEventHandler() = default;
};

// Reactor loop.
//
// Companion class to the event loop in `loop.h` that executes
// functions on triggers. The triggers are posted to a shared memory
// ring buffer, so this can be done by other processes on the same
// machine. It uses extra data in the ring buffer header to store a
// mutex and condition variable to avoid a busy loop.
//
class Reactor final : public DeferredExecutor {
 public:
  using TFunction = std::function<void()>;
  using TToken = uint32_t;

  Reactor();

  using TDeferredFunction = std::function<void()>;

  // Run function on reactor thread.
  // If the function throws, the thread crashes.
  void deferToLoop(TDeferredFunction fn) override;

  IbvLib& getIbvLib() {
    return ibvLib_;
  }

  IbvContext& getIbvContext() {
    return ctx_;
  }

  IbvProtectionDomain& getIbvPd() {
    return pd_;
  }

  IbvCompletionQueue& getIbvCq() {
    return cq_;
  }

  IbvSharedReceiveQueue& getIbvSrq() {
    return srq_;
  }

  IbvAddress& getIbvAddress() {
    return addr_;
  }

  void registerQp(uint32_t qpn, std::shared_ptr<IbvEventHandler> eventHandler);

  void unregisterQp(uint32_t qpn);

  void postWrite(IbvQueuePair& qp, IbvLib::send_wr& wr);

  void postAck(IbvQueuePair& qp, IbvLib::send_wr& wr);

  inline bool inLoop() override {
    {
      std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
      if (likely(isThreadConsumingDeferredFunctions_)) {
        return std::this_thread::get_id() == thread_.get_id();
      }
    }
    return onDemandLoop_.inLoop();
  }

  bool isViable() const;

  void setId(std::string id);

  void close();

  void join();

  ~Reactor();

 private:
  // InfiniBand stuff
  bool foundIbvLib_{false};
  IbvLib ibvLib_;
  IbvContext ctx_;
  IbvProtectionDomain pd_;
  IbvCompletionQueue cq_;
  IbvSharedReceiveQueue srq_;
  IbvAddress addr_;

  void postRecvRequestsOnSRQ_(int num);

  std::mutex mutex_;
  std::thread thread_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  std::mutex deferredFunctionMutex_;
  std::list<TDeferredFunction> deferredFunctionList_;
  std::atomic<int64_t> deferredFunctionCount_{0};

  // An identifier for the context, composed of the identifier for the context,
  // combined with the transport's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  // Whether the thread is still taking care of running the deferred functions
  //
  // This is part of what can only be described as a hack. Sometimes, even when
  // using the API as intended, objects try to defer tasks to the loop after
  // that loop has been closed and joined. Since those tasks may be lambdas that
  // captured shared_ptrs to the objects in their closures, this may lead to a
  // reference cycle and thus a leak. Our hack is to have this flag to record
  // when we can no longer defer tasks to the loop and in that case we just run
  // those tasks inline. In order to keep ensuring the single-threadedness
  // assumption of our model (which is what we rely on to be safe from race
  // conditions) we use an on-demand loop.
  bool isThreadConsumingDeferredFunctions_{true};
  OnDemandDeferredExecutor onDemandLoop_;

  // Reactor thread entry point.
  void run();

  // The registered event handlers for each queue pair.
  std::unordered_map<uint32_t, std::shared_ptr<IbvEventHandler>>
      queuePairEventHandler_;

  uint32_t numAvailableWrites_{kNumPendingWriteReqs};
  uint32_t numAvailableAcks_{kNumPendingAckReqs};
  std::deque<std::tuple<IbvQueuePair&, IbvLib::send_wr>> pendingQpWrites_;
  std::deque<std::tuple<IbvQueuePair&, IbvLib::send_wr>> pendingQpAcks_;
};

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
