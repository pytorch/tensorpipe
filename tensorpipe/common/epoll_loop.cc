/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/epoll_loop.h>

#include <sys/eventfd.h>

#include <tensorpipe/common/system.h>

namespace tensorpipe {

EpollLoop::EpollLoop(DeferredExecutor& deferredExecutor)
    : deferredExecutor_(deferredExecutor) {
  {
    auto rv = ::epoll_create(1);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
    epollFd_ = Fd(rv);
  }
  {
    auto rv = ::eventfd(0, EFD_NONBLOCK);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
    eventFd_ = Fd(rv);
  }

  // Register the eventfd with epoll.
  {
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.u64 = 0;
    auto rv = ::epoll_ctl(epollFd_.fd(), EPOLL_CTL_ADD, eventFd_.fd(), &ev);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
  }

  // Start epoll(2) thread.
  thread_ = std::thread(&EpollLoop::loop, this);
}

void EpollLoop::close() {
  if (!closed_.exchange(true)) {
    wakeup();
  }
}

void EpollLoop::join() {
  close();

  if (!joined_.exchange(true)) {
    thread_.join();
  }
}

EpollLoop::~EpollLoop() {
  join();

  // Unregister the eventfd with epoll.
  {
    auto rv = ::epoll_ctl(epollFd_.fd(), EPOLL_CTL_DEL, eventFd_.fd(), nullptr);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
  }
}

void EpollLoop::registerDescriptor(
    int fd,
    int events,
    std::shared_ptr<EventHandler> h) {
  TP_DCHECK(deferredExecutor_.inLoop());

  std::lock_guard<std::mutex> lock(handlersMutex_);

  uint64_t record = nextRecord_++;

  struct epoll_event ev;
  ev.events = events;
  ev.data.u64 = record;

  auto fdIter = fdToRecord_.find(fd);
  if (fdIter == fdToRecord_.end()) {
    fdToRecord_.emplace(fd, record);
    recordToHandler_.emplace(record, h);

    auto rv = ::epoll_ctl(epollFd_.fd(), EPOLL_CTL_ADD, fd, &ev);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
  } else {
    uint64_t oldRecord = fdIter->second;
    fdIter->second = record;
    recordToHandler_.erase(oldRecord);
    recordToHandler_.emplace(record, h);

    auto rv = ::epoll_ctl(epollFd_.fd(), EPOLL_CTL_MOD, fd, &ev);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
  }
}

void EpollLoop::unregisterDescriptor(int fd) {
  TP_DCHECK(deferredExecutor_.inLoop());

  std::lock_guard<std::mutex> lock(handlersMutex_);

  auto fdIter = fdToRecord_.find(fd);
  TP_DCHECK(fdIter != fdToRecord_.end());
  uint64_t oldRecord = fdIter->second;
  fdToRecord_.erase(fdIter);
  recordToHandler_.erase(oldRecord);

  auto rv = ::epoll_ctl(epollFd_.fd(), EPOLL_CTL_DEL, fd, nullptr);
  TP_THROW_SYSTEM_IF(rv == -1, errno);

  // Maybe we're done and the event loop is waiting for the last handlers to
  // be unregistered before terminating, so just in case we wake it up.
  if (fdToRecord_.empty()) {
    wakeup();
  }
}

void EpollLoop::wakeup() {
  // Perform a write to eventfd to wake up epoll_wait(2).
  eventFd_.writeOrThrow<uint64_t>(1);
}

bool EpollLoop::hasRegisteredHandlers() {
  std::lock_guard<std::mutex> lock(handlersMutex_);
  TP_DCHECK_EQ(fdToRecord_.size(), recordToHandler_.size());
  return !fdToRecord_.empty();
}

void EpollLoop::loop() {
  setThreadName("TP_IBV_loop");

  // Stop when another thread has asked the loop the close and when all
  // handlers have been unregistered except for the wakeup eventfd one.
  while (!closed_ || hasRegisteredHandlers()) {
    // Use fixed epoll_event capacity for every call.
    std::vector<struct epoll_event> epollEvents(kCapacity);

    // Block waiting for something to happen...
    auto nfds =
        ::epoll_wait(epollFd_.fd(), epollEvents.data(), epollEvents.size(), -1);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      TP_THROW_SYSTEM(errno);
    }

    // Always immediately read from the eventfd so that it is no longer readable
    // on the next call to epoll_wait(2). As it's opened in non-blocking mode,
    // reading from it if its value is zero just return EAGAIN. Reset it before
    // invoking any of the callbacks, so that if they perform a wakeup they will
    // wake up the next iteration of epoll_wait(2).
    {
      uint64_t val;
      auto rv = eventFd_.read(reinterpret_cast<void*>(&val), sizeof(val));
      TP_DCHECK(
          (rv == -1 && errno == EAGAIN) || (rv == sizeof(val) && val > 0));
    }

    // Resize based on actual number of events.
    epollEvents.resize(nfds);

    // Defer handling to reactor and wait for it to process these events.
    deferredExecutor_.runInLoop(
        [this, epollEvents{std::move(epollEvents)}]() mutable {
          handleEpollEventsFromLoop(std::move(epollEvents));
        });
  }
}

void EpollLoop::handleEpollEventsFromLoop(
    std::vector<struct epoll_event> epollEvents) {
  TP_DCHECK(deferredExecutor_.inLoop());

  // Process events returned by epoll_wait(2).
  for (const auto& event : epollEvents) {
    const uint64_t record = event.data.u64;

    // Make a copy so that if the handler unregisters itself as it runs it will
    // still be kept alive by our copy of the shared_ptr.
    std::shared_ptr<EventHandler> handler;
    {
      std::unique_lock<std::mutex> handlersLock(handlersMutex_);
      const auto recordIter = recordToHandler_.find(record);
      if (recordIter == recordToHandler_.end()) {
        continue;
      }
      handler = recordIter->second;
    }

    handler->handleEventsFromLoop(event.events);
  }
}

std::string EpollLoop::formatEpollEvents(uint32_t events) {
  std::string res;
  if (events & EPOLLIN) {
    res = res.empty() ? "IN" : res + " | IN";
    events &= ~EPOLLIN;
  }
  if (events & EPOLLOUT) {
    res = res.empty() ? "OUT" : res + " | OUT";
    events &= ~EPOLLOUT;
  }
  if (events & EPOLLERR) {
    res = res.empty() ? "ERR" : res + " | ERR";
    events &= ~EPOLLERR;
  }
  if (events & EPOLLHUP) {
    res = res.empty() ? "HUP" : res + " | HUP";
    events &= ~EPOLLHUP;
  }
  if (events > 0) {
    std::string eventsStr = std::to_string(events);
    res = res.empty() ? eventsStr : res + " | " + eventsStr;
  }
  return res;
}

} // namespace tensorpipe
