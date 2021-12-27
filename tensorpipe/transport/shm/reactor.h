/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/ringbuffer_role.h>
#include <tensorpipe/common/shm_segment.h>

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
class Reactor final : public BusyPollingLoop {
  // This allows for buffering 1M triggers (at 4 bytes a piece).
  static constexpr auto kSize = 4 * 1024 * 1024;

  static constexpr int kNumRingbufferRoles = 2;

 public:
  using TFunction = std::function<void()>;
  using TToken = uint32_t;
  using Consumer = RingBufferRole<kNumRingbufferRoles, 0>;
  using Producer = RingBufferRole<kNumRingbufferRoles, 1>;

  Reactor();

  // Add function to the reactor.
  // Returns token that can be used to trigger it.
  TToken add(TFunction fn);

  // Removes function associated with token from reactor.
  void remove(TToken token);

  // Returns the file descriptors for the underlying ring buffer.
  std::tuple<int, int> fds() const;

  void close();

  void join();

  ~Reactor();

 protected:
  bool pollOnce() override;

  bool readyToClose() override;

 private:
  ShmSegment headerSegment_;
  ShmSegment dataSegment_;
  RingBuffer<kNumRingbufferRoles> rb_;

  std::mutex mutex_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

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
    Trigger(Fd header, Fd data);

    void run(TToken token);

   private:
    ShmSegment headerSegment_;
    ShmSegment dataSegment_;
    RingBuffer<kNumRingbufferRoles> rb_;
  };
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
