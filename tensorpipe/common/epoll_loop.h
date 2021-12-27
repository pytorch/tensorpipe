/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sys/epoll.h>

#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/fd.h>

namespace tensorpipe {

class EpollLoop final {
 public:
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

  explicit EpollLoop(DeferredExecutor& deferredExecutor);

  // Register file descriptor with event loop.
  //
  // Trigger the handler if any of the epoll events in the `events`
  // mask occurs. If an event is triggered, the loop first acquires a
  // copy of the shared_ptr to the handler before calling into its
  // handler function. This ensures that the handler is alive for the
  // duration of this function.
  //
  void registerDescriptor(int fd, int events, std::shared_ptr<EventHandler> h);

  // Unregister file descriptor from event loop.
  //
  // This resets the shared_ptr to the event handler that was registered
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

  ~EpollLoop();

  static std::string formatEpollEvents(uint32_t events);

 private:
  static constexpr auto kCapacity = 64;

  // The reactor is used to process events for this loop.
  DeferredExecutor& deferredExecutor_;

  // Wake up the event loop.
  void wakeup();

  // Main loop function.
  void loop();

  // Check whether some handlers are currently registered.
  bool hasRegisteredHandlers();

  Fd epollFd_;
  Fd eventFd_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};
  std::thread thread_;

  // Interaction with epoll(7).
  //
  // A dedicated thread runs epoll_wait(2) in a loop and, every time it returns,
  // it defers a function to the reactor which is responsible for processing the
  // epoll events and executing the handlers, and then notify the epoll thread
  // that it is done, for it to start another iteration. This back-and-forth
  // between these threads is done to ensure that all epoll handlers are run
  // from the reactor thread, just like everything else. Doing so makes it
  // easier to reason about how certain events are sequenced. For example, if
  // another processes first makes a write to a connection and then closes the
  // accompanying Unix domain socket, we know for a fact that the reactor will
  // first react to the write, and then react to the epoll event caused by
  // closing the socket. If we didn't force serialization onto the reactor, we
  // would not have this guarantee.
  //
  // It's safe to call epoll_ctl from one thread while another thread is blocked
  // on an epoll_wait call. This means that the kernel internally serializes the
  // operations on a single epoll fd. However, we have no way to control whether
  // a modification of the set of file descriptors monitored by epoll occurred
  // just before or just after the return from the epoll_wait. This means that
  // when we start processing the result of epoll_wait we can't know what set of
  // file descriptors it operated on. This becomes a problem if, for example, in
  // between the moment epoll_wait returns and the moment we process the results
  // a file descriptor is unregistered and closed and another one with the same
  // value is opened and registered: we'd end up calling the handler of the new
  // fd for the events of the old one (which probably include errors).
  //
  // However, epoll offers a way to address this: epoll_wait returns, for each
  // event, the piece of extra data that was provided by the *last* call on
  // epoll_ctl for that fd. This allows us to detect whether epoll_wait had
  // taken into account an update to the set of fds or not. We do so by giving
  // each update a unique identifier, called "record". Each update to a fd will
  // associate a new record to it. The handlers are associated to records (and
  // not to fds), and for each fd we know which handler is the one currently
  // installed. This way when processing an event we can detect whether the
  // record for that event is still valid or whether it is stale, in which case
  // we disregard the event, and wait for it to fire again at the next epoll
  // iteration, with the up-to-date handler.
  std::unordered_map<int, uint64_t> fdToRecord_;
  std::unordered_map<uint64_t, std::shared_ptr<EventHandler>> recordToHandler_;
  uint64_t nextRecord_{1}; // Reserve record 0 for the eventfd
  std::mutex handlersMutex_;

  // Deferred to the reactor to handle the events received by epoll_wait(2).
  void handleEpollEventsFromLoop(std::vector<struct epoll_event> epollEvents);
};

} // namespace tensorpipe
