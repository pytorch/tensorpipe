/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <sys/eventfd.h>

#include <deque>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/shm/loop.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport::shm;

namespace {

class Handler : public EventHandler {
 public:
  void handleEventsFromLoop(int events) override {
    std::unique_lock<std::mutex> lock(m_);
    events_.push_back(events);
    cv_.notify_all();
  }

  int nextEvents() {
    std::unique_lock<std::mutex> lock(m_);
    while (events_.empty()) {
      cv_.wait(lock);
    }
    int events = events_.front();
    events_.pop_front();
    return events;
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::deque<int> events_;
};

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

  FunctionEventHandler(
      Reactor& reactor,
      Loop& loop,
      int fd,
      int event,
      TFunction fn);

  ~FunctionEventHandler() override;

  void start();

  void cancel();

  void handleEventsFromLoop(int events) override;

 private:
  Reactor& reactor_;
  Loop& loop_;
  const int fd_;
  const int event_;
  TFunction fn_;

  std::mutex mutex_;
  bool cancelled_{false};
};

FunctionEventHandler::FunctionEventHandler(
    Reactor& reactor,
    Loop& loop,
    int fd,
    int event,
    TFunction fn)
    : reactor_(reactor),
      loop_(loop),
      fd_(fd),
      event_(event),
      fn_(std::move(fn)) {}

FunctionEventHandler::~FunctionEventHandler() {
  cancel();
}

void FunctionEventHandler::start() {
  reactor_.runInLoop(
      [&]() { loop_.registerDescriptor(fd_, event_, shared_from_this()); });
}

void FunctionEventHandler::cancel() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!cancelled_) {
    reactor_.runInLoop([&]() {
      loop_.unregisterDescriptor(fd_);
      cancelled_ = true;
    });
  }
}

void FunctionEventHandler::handleEventsFromLoop(int events) {
  if (events & event_) {
    fn_(*this);
  }
}

// Instantiates an event monitor for the specified fd.
template <typename T>
std::shared_ptr<FunctionEventHandler> createMonitor(
    Reactor& reactor,
    Loop& loop,
    std::shared_ptr<T> shared,
    int fd,
    int event,
    std::function<void(T&, FunctionEventHandler&)> fn) {
  auto handler = std::make_shared<FunctionEventHandler>(
      reactor,
      loop,
      fd,
      event,
      [weak{std::weak_ptr<T>{shared}},
       fn{std::move(fn)}](FunctionEventHandler& handler) {
        auto shared = weak.lock();
        if (shared) {
          fn(*shared, handler);
        }
      });
  handler->start();
  return handler;
}

} // namespace

TEST(Loop, RegisterUnregister) {
  Reactor reactor;
  Loop loop{reactor};
  auto handler = std::make_shared<Handler>();
  auto efd = Fd(eventfd(0, EFD_NONBLOCK));

  {
    // Test if writable (always).
    reactor.runInLoop([&]() {
      loop.registerDescriptor(efd.fd(), EPOLLOUT | EPOLLONESHOT, handler);
    });
    ASSERT_EQ(handler->nextEvents(), EPOLLOUT);
    efd.writeOrThrow<uint64_t>(1337);

    // Test if readable (only if previously written to).
    reactor.runInLoop([&]() {
      loop.registerDescriptor(efd.fd(), EPOLLIN | EPOLLONESHOT, handler);
    });
    ASSERT_EQ(handler->nextEvents(), EPOLLIN);
    ASSERT_EQ(efd.readOrThrow<uint64_t>(), 1337);

    // Test if we can unregister the descriptor.
    reactor.runInLoop([&]() { loop.unregisterDescriptor(efd.fd()); });
  }

  loop.join();
}

TEST(Loop, Monitor) {
  Reactor reactor;
  Loop loop{reactor};
  auto efd = Fd(eventfd(0, EFD_NONBLOCK));
  constexpr uint64_t kValue = 1337;

  {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;

    // Test if writable (always).
    auto shared = std::make_shared<int>(1338);
    auto monitor = createMonitor<int>(
        reactor,
        loop,
        shared,
        efd.fd(),
        EPOLLOUT,
        [&](int& i, FunctionEventHandler& handler) {
          EXPECT_EQ(i, 1338);
          efd.writeOrThrow<uint64_t>(kValue);
          handler.cancel();
          {
            std::unique_lock<std::mutex> lock(mutex);
            done = true;
            cv.notify_all();
          }
        });

    // Wait for monitor to trigger and perform a write.
    std::unique_lock<std::mutex> lock(mutex);
    while (!done) {
      cv.wait(lock);
    }
  }

  {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    uint64_t value = 0;

    // Test if readable (only if previously written to).
    auto shared = std::make_shared<int>(1338);
    auto monitor = createMonitor<int>(
        reactor,
        loop,
        shared,
        efd.fd(),
        EPOLLIN,
        [&](int& i, FunctionEventHandler& handler) {
          EXPECT_EQ(i, 1338);
          value = efd.readOrThrow<uint64_t>();
          handler.cancel();
          {
            std::unique_lock<std::mutex> lock(mutex);
            done = true;
            cv.notify_all();
          }
        });

    // Wait for monitor to trigger and perform a read.
    std::unique_lock<std::mutex> lock(mutex);
    while (!done) {
      cv.wait(lock);
    }

    // Verify we read the correct value.
    ASSERT_EQ(value, kValue);
  }

  loop.join();
}

TEST(Loop, Defer) {
  Reactor reactor;
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  reactor.deferToLoop([promise]() { promise->set_value(); });
  future.wait();
  ASSERT_TRUE(future.valid());
  reactor.join();
}
