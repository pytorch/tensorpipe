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
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/shm/fd.h>
#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>

namespace tensorpipe {
namespace transport {
namespace shm {

// Reactor loop.
//
// Companion class to the event loop in `loop.h` that executes
// functions on triggers. The triggers are posted to a shared memory
// ring buffer, so this can be done by other processes on the same
// machine. It uses extra data in the ring buffer header to store a
// mutex and condition variable to avoid a busy loop.
//
class Reactor final {
  // This allows for buffering 1M triggers (at 4 bytes a piece).
  static constexpr auto kSize = 4 * 1024 * 1024;

 public:
  using TFunction = std::function<void()>;
  using TToken = uint32_t;

  Reactor();

  using TDeferredFunction = std::function<void()>;

  // Run function on reactor thread.
  // If the function throws, the thread crashes.
  void deferToLoop(TDeferredFunction fn);

  // Prefer using deferToLoop over runInLoop when you don't need to wait for the
  // result.
  template <typename F>
  void runInLoop(F&& fn) {
    // When called from the event loop thread itself (e.g., from a callback),
    // deferring would cause a deadlock because the given callable can only be
    // run when the loop is allowed to proceed. On the other hand, it means it
    // is thread-safe to run it immediately. The danger here however is that it
    // can lead to an inconsistent order between operations run from the event
    // loop, from outside of it, and deferred.
    if (inReactorThread()) {
      fn();
    } else {
      // Must use a copyable wrapper around std::promise because
      // we use it from a std::function which must be copyable.
      auto promise = std::make_shared<std::promise<void>>();
      auto future = promise->get_future();
      deferToLoop([promise, fn{std::forward<F>(fn)}]() {
        try {
          fn();
          promise->set_value();
        } catch (...) {
          promise->set_exception(std::current_exception());
        }
      });
      future.get();
    }
  }

  // Add function to the reactor.
  // Returns token that can be used to trigger it.
  TToken add(TFunction fn);

  // Removes function associated with token from reactor.
  void remove(TToken token);

  // Trigger reactor with specified token.
  void trigger(TToken token);

  // Returns the file descriptors for the underlying ring buffer.
  std::tuple<int, int> fds() const;

  inline bool inReactorThread() {
    {
      std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
      if (likely(isThreadConsumingDeferredFunctions_)) {
        return std::this_thread::get_id() == thread_.get_id();
      }
    }
    return onDemandLoop_.inLoop();
  }

  void close();

  void join();

  ~Reactor();

 private:
  Fd headerFd_;
  Fd dataFd_;
  optional<util::ringbuffer::Consumer> consumer_;
  optional<util::ringbuffer::Producer> producer_;

  std::mutex mutex_;
  std::thread thread_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  std::mutex deferredFunctionMutex_;
  std::list<TDeferredFunction> deferredFunctionList_;
  std::atomic<int64_t> deferredFunctionCount_{0};

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
  OnDemandLoop onDemandLoop_;

  // Reactor thread entry point.
  void run();

  // Tokens are placed in this set if they can be reused.
  std::set<TToken> reusableTokens_;

  // Map reactor tokens to functions.
  //
  // The tokens are reused so we don't worry about unbounded growth
  // and comfortably use a std::vector here.
  //
  std::vector<TFunction> functions_;

  // Count how many functions are registered.
  std::atomic<uint64_t> functionCount_{0};

 public:
  class Trigger {
   public:
    Trigger(Fd&& header, Fd&& data);

    void run(TToken token);

   private:
    util::ringbuffer::Producer producer_;
  };
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
