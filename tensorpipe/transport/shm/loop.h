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

#include <tensorpipe/transport/shm/fd.h>

namespace tensorpipe {
namespace transport {
namespace shm {

// Abstract base class called by the epoll(2) event loop.
//
// Dispatch to multiple types is needed because we must deal with a
// few listening sockets and an eventfd(2) per connection.
//
class EventHandler {
 public:
  virtual ~EventHandler() = default;

  virtual void handleEvents(int events) = 0;
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

  FunctionEventHandler(
      std::shared_ptr<Loop> loop,
      int fd,
      int event,
      TFunction fn);

  ~FunctionEventHandler() override;

  void start();

  void cancel();

  void handleEvents(int events) override;

 private:
  std::shared_ptr<Loop> loop_;
  const int fd_;
  const int event_;
  TFunction fn_;

  std::mutex mutex_;
  bool cancelled_{false};
};

class Loop final : public std::enable_shared_from_this<Loop> {
 public:
  using TFunction = std::function<void()>;

  explicit Loop();

  ~Loop();

  void registerDescriptor(int fd, int events, std::shared_ptr<EventHandler> h);

  void unregisterDescriptor(int fd);

  std::future<void> run(TFunction fn);

  // Returns if the calling thread is the same as the loop thread.
  bool isThisTheLoopThread() const;

  // Instantiates an event monitor for the specified fd.
  template <typename T>
  std::shared_ptr<FunctionEventHandler> monitor(
      std::shared_ptr<T> shared,
      int fd,
      int event,
      std::function<void(T&, FunctionEventHandler&)> fn) {
    auto handler = std::make_shared<FunctionEventHandler>(
        shared_from_this(),
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

 private:
  static constexpr auto capacity_ = 64;

  // Wake up the event loop.
  void wakeup();

  // Main loop function.
  void loop();

  Fd epollFd_;
  Fd eventFd_;
  uint64_t loopTicks_{0};
  std::atomic<bool> done_{false};
  std::unique_ptr<std::thread> loop_;

  std::mutex m_;

  // Store weak_ptr for every registered fd.
  std::vector<std::weak_ptr<EventHandler>> handlers_;
  std::mutex handlersMutex_;

  // List of functions to run on the next event loop tick.
  std::list<TFunction> functions_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
