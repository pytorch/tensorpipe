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
#include <list>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

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
  // This allows for buffering 2k triggers (at 4 bytes a piece).
  static constexpr auto kSize = 8192;

 public:
  using TFunction = std::function<void()>;
  using TToken = uint32_t;

  explicit Reactor();

  using TDeferredFunction = std::function<void()>;

  // Run function on reactor thread.
  // If the function throws, the thread crashes.
  void deferToLoop(TDeferredFunction fn);

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
    return std::this_thread::get_id() == thread_.get_id();
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

  TToken deferredFunctionToken_;
  std::mutex deferredFunctionMutex_;
  std::list<TDeferredFunction> deferredFunctionList_;

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

  // Called in response to a deferred function.
  void handleDeferredFunctionFromLoop();

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
