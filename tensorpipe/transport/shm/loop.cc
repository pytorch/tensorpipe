/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/loop.h>

#include <sys/eventfd.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

// Checks if the specified weak_ptr is uninitialized.
template <typename T>
bool is_uninitialized(const std::weak_ptr<T>& weak) {
  const std::weak_ptr<T> empty{};
  return !weak.owner_before(empty) && !empty.owner_before(weak);
}

} // namespace

FunctionEventHandler::FunctionEventHandler(
    Loop* loop,
    int fd,
    int event,
    TFunction fn)
    : loop_(loop), fd_(fd), event_(event), fn_(std::move(fn)) {}

FunctionEventHandler::~FunctionEventHandler() {
  cancel();
}

void FunctionEventHandler::start() {
  loop_->registerDescriptor(fd_, event_, shared_from_this());
}

void FunctionEventHandler::cancel() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!cancelled_) {
    loop_->unregisterDescriptor(fd_);
    cancelled_ = true;
  }
}

void FunctionEventHandler::handleEventsFromLoop(int events) {
  if (events & event_) {
    fn_(*this);
  }
}

std::shared_ptr<Loop> Loop::create() {
  return std::make_shared<Loop>(ConstructorToken());
}

Loop::Loop(ConstructorToken /* unused */) {
  {
    auto rv = epoll_create(1);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
    epollFd_ = Fd(rv);
  }
  {
    auto rv = eventfd(0, EFD_NONBLOCK);
    TP_THROW_SYSTEM_IF(rv == -1, errno);
    eventFd_ = Fd(rv);
  }

  // Create reactor.
  reactor_ = std::make_shared<Reactor>();
  epollReactorToken_ = reactor_->add([this] { handleEpollEventsFromLoop(); });
  deferredFunctionReactorToken_ =
      reactor_->add([this] { handleDeferredFunctionFromLoop(); });

  // Start epoll(2) thread.
  thread_ = std::thread(&Loop::loop, this);
}

void Loop::close() {
  bool wasClosed = false;
  closed_.compare_exchange_strong(wasClosed, true);
  if (!wasClosed) {
    closingEmitter_.close();
    wakeup();
  }
}

void Loop::join() {
  close();

  bool wasJoined = false;
  joined_.compare_exchange_strong(wasJoined, true);
  if (!wasJoined) {
    thread_.join();
  }
}

Loop::~Loop() {
  join();
}

void Loop::deferToLoop(TDeferredFunction fn) {
  std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
  deferredFunctionList_.push_back(std::move(fn));
  reactor_->trigger(deferredFunctionReactorToken_);
}

const std::shared_ptr<Reactor>& Loop::reactor() {
  return reactor_;
}

void Loop::registerDescriptor(
    int fd,
    int events,
    std::shared_ptr<EventHandler> h) {
  struct epoll_event ev;
  ev.events = events;
  ev.data.fd = fd;

  {
    std::lock_guard<std::mutex> lock(handlersMutex_);
    if (fd >= handlers_.size()) {
      handlers_.resize(fd + 1);
    }
    if (is_uninitialized(handlers_[fd])) {
      handlerCount_++;
    }
    handlers_[fd] = h;
  }

  auto rv = epoll_ctl(epollFd_.fd(), EPOLL_CTL_ADD, fd, &ev);
  if (rv == -1 && errno == EEXIST) {
    rv = epoll_ctl(epollFd_.fd(), EPOLL_CTL_MOD, fd, &ev);
  }
  TP_THROW_SYSTEM_IF(rv == -1, errno);
}

void Loop::unregisterDescriptor(int fd) {
  auto rv = epoll_ctl(epollFd_.fd(), EPOLL_CTL_DEL, fd, nullptr);
  TP_THROW_SYSTEM_IF(rv == -1, errno);

  {
    std::lock_guard<std::mutex> lock(handlersMutex_);
    if (!is_uninitialized(handlers_[fd])) {
      handlerCount_--;
    }
    handlers_[fd].reset();
    // Maybe we're done and the event loop is waiting for the last handlers to
    // be unregistered before terminating, so just in case we wake it up.
    if (handlerCount_ == 1) {
      wakeup();
    }
  }
}

void Loop::wakeup() {
  // Perform a write to eventfd to wake up epoll_wait(2).
  eventFd_.writeOrThrow<uint64_t>(1);
}

void Loop::loop() {
  // Monitor eventfd for readability. Always read from the eventfd so
  // that it is no longer readable on the next call to epoll_wait(2).
  // Note: this is allocated on the stack so that we destroy it upon
  // terminating the event loop thread.
  auto wakeupHandler = std::make_shared<FunctionEventHandler>(
      this, eventFd_.fd(), EPOLLIN, [this](FunctionEventHandler& /* unused */) {
        eventFd_.readOrThrow<uint64_t>();
      });
  wakeupHandler->start();

  std::unique_lock<std::mutex> lock(epollMutex_);
  for (;;) {
    // Use fixed epoll_event capacity for every call.
    epollEvents_.resize(kCapacity_);

    // Block waiting for something to happen...
    auto nfds =
        epoll_wait(epollFd_.fd(), epollEvents_.data(), epollEvents_.size(), -1);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      TP_THROW_SYSTEM(errno);
    }

    // Resize based on actual number of events.
    epollEvents_.resize(nfds);

    // Trigger reactor and wait for it to process these events.
    reactor_->trigger(epollReactorToken_);
    while (!epollEvents_.empty()) {
      epollCond_.wait(lock);
    }

    // Return if another thread is waiting in `join` and there is
    // nothing left to be done. The handler count is equal to 1
    // because we're always monitoring the eventfd for wakeups.
    if (closed_ && handlerCount_ == 1) {
      return;
    }
  }
}

void Loop::handleEpollEventsFromLoop() {
  std::unique_lock<std::mutex> lock(epollMutex_);

  // Process events returned by epoll_wait(2).
  {
    std::unique_lock<std::mutex> lock(handlersMutex_);
    for (const auto& event : epollEvents_) {
      const auto fd = event.data.fd;
      auto h = handlers_[fd].lock();
      if (h) {
        lock.unlock();
        // Trigger callback. Note that the object is kept alive
        // through the shared_ptr that we acquired by locking the
        // weak_ptr in the handlers vector.
        h->handleEventsFromLoop(event.events);
        // Reset the handler shared_ptr before reacquiring the lock.
        // This may trigger destruction of the object.
        h.reset();
        lock.lock();
      }
    }
  }

  // Let epoll thread know we've completed processing.
  epollEvents_.clear();
  epollCond_.notify_one();
}

void Loop::handleDeferredFunctionFromLoop() {
  std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
  auto it = deferredFunctionList_.begin();
  TP_DCHECK(it != deferredFunctionList_.end());
  auto fn = std::move(*it);
  deferredFunctionList_.erase(it);

  // Unlock before executing, because the function could try to
  // defer another function and that needs the lock.
  lock.unlock();
  fn();
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
