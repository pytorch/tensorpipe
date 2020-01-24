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

  FunctionEventHandler(Loop* loop, int fd, int event, TFunction fn);

  ~FunctionEventHandler() override;

  void start();

  void cancel();

  void handleEvents(int events) override;

 private:
  Loop* loop_;
  const int fd_;
  const int event_;
  TFunction fn_;

  std::mutex mutex_;
  bool cancelled_{false};
};

class Loop final : public std::enable_shared_from_this<Loop> {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Loop> create();

  using TFunction = std::function<void()>;

  explicit Loop(ConstructorToken);

  ~Loop();

  void registerDescriptor(int fd, int events, std::shared_ptr<EventHandler> h);

  void unregisterDescriptor(int fd);

  std::future<void> run(TFunction fn);

  // Instantiates an event monitor for the specified fd.
  template <typename T>
  std::shared_ptr<FunctionEventHandler> monitor(
      std::shared_ptr<T> shared,
      int fd,
      int event,
      std::function<void(T&, FunctionEventHandler&)> fn) {
    // Note: we capture a shared_ptr to the loop in the lambda below
    // in order to keep the loop alive from the function event handler
    // instance. We cannot have the instance store a shared_ptr to the
    // loop itself, because that would cause a reference cycle when
    // the loop stores an instance itself.
    auto handler = std::make_shared<FunctionEventHandler>(
        this,
        fd,
        event,
        [loop{shared_from_this()},
         weak{std::weak_ptr<T>{shared}},
         fn{std::move(fn)}](FunctionEventHandler& handler) {
          auto shared = weak.lock();
          if (shared) {
            fn(*shared, handler);
          }
        });
    handler->start();
    return handler;
  }

  // Tell loop to terminate when no more handlers remain.
  void join();

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
  std::atomic<uint64_t> handlerCount_{0};

  // List of functions to run on the next event loop tick.
  std::list<TFunction> functions_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
