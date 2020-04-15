/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <sys/epoll.h>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/shm/fd.h>
#include <tensorpipe/transport/shm/reactor.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Connection;
class Listener;

// Abstract base class called by the epoll(2) event loop.
//
// Dispatch to multiple types is needed because we must deal with a
// few listening sockets and an eventfd(2) per connection.
//
class EventHandler {
 public:
  virtual ~EventHandler() = default;

  virtual void handleEventsFromLoop(int events) = 0;
};

class Loop;

// Monitor an fd for events and execute function when triggered.
//
// The lifetime of an instance dictates when the specified function
// may be called. The function is guaranteed to not be called after
// the monitor has been destructed.
//
class FunctionEventHandler
    : public EventHandler,
      public std::enable_shared_from_this<FunctionEventHandler> {
 public:
  using TFunction = std::function<void(FunctionEventHandler&)>;

  FunctionEventHandler(Loop* loop, int fd, int event, TFunction fn);

  ~FunctionEventHandler() override;

  void start();

  void cancel();

  void handleEventsFromLoop(int events) override;

 private:
  Loop* loop_;
  const int fd_;
  const int event_;
  TFunction fn_;

  std::mutex mutex_;
  bool cancelled_{false};
};

class Loop final {
 public:
  using TDeferredFunction = std::function<void()>;

  Loop();

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
    if (reactor_.inReactorThread()) {
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

  // Run function on reactor thread.
  // If the function throws, the thread crashes.
  void deferToLoop(TDeferredFunction fn);

  // Provide access to the underlying reactor.
  Reactor& reactor();

  // Register file descriptor with event loop.
  //
  // Trigger the handler if any of the epoll events in the `events`
  // mask occurs. The loop stores a weak_ptr to the handler, so it is
  // the responsibility of the caller to keep the handler alive. If an
  // event is triggered, the loop first acquires a shared_ptr to the
  // handler before calling into its handler function. This ensures
  // that the handler is alive for the duration of this function.
  //
  void registerDescriptor(int fd, int events, std::shared_ptr<EventHandler> h);

  // Unregister file descriptor from event loop.
  //
  // This resets the weak_ptr to the event handler that was registered
  // in `registerDescriptor`. Upon returning, the handler can no
  // longer be called, even if there were pending events for the file
  // descriptor. Only if the loop had acquired a shared_ptr to the
  // handler prior to this function being called, can the handler
  // function still be called.
  //
  void unregisterDescriptor(int fd);

  void close();

  // Tell loop to terminate when no more handlers remain.
  void join();

  ~Loop();

  inline bool inLoopThread() {
    return reactor_.inReactorThread();
  }

 private:
  static constexpr auto kCapacity_ = 64;

  // The reactor is used to process events for this loop.
  Reactor reactor_;

  // Interaction with epoll(7).
  //
  // A dedicated thread runs epoll_wait(2) in a loop and triggers the
  // reactor every time it returns. The function registered with the
  // reactor is responsible for processing the epoll events and
  // notifying the epoll thread that it is done. This back-and-forth
  // between these threads is done to ensure that both events from
  // epoll and events posted to the reactor are handled by a single
  // thread. Doing so makes it easier to reason about how certain
  // events are sequenced. For example, if another processes first
  // makes a write to a connection and then closes the accompanying
  // Unix domain socket, we know for a fact that the reactor will
  // first react to the write, and then react to the epoll event
  // caused by closing the socket. If we didn't force serialization
  // onto the reactor, we would not have this guarantee.
  //
  Reactor::TToken epollReactorToken_;
  std::mutex epollMutex_;
  std::condition_variable epollCond_;
  std::vector<struct epoll_event> epollEvents_;

  // Wake up the event loop.
  void wakeup();

  // Main loop function.
  void loop();

  Fd epollFd_;
  Fd eventFd_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};
  std::mutex mutex_;
  std::thread thread_;

  // Store weak_ptr for every registered fd.
  std::vector<std::weak_ptr<EventHandler>> handlers_;
  std::mutex handlersMutex_;
  std::atomic<uint64_t> handlerCount_{0};

  // Called by the reactor in response to epoll_wait(2) producing a
  // vector with epoll_event structs in `epollEvents_`.
  void handleEpollEventsFromLoop();

  friend class Connection;
  friend class Listener;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
