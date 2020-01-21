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
// the monitor has been destructed. If the monitor hasn't been
// cancelled already, it is cancelled from the destructor. Be aware
// that this operation can block and wait for the event loop thread.
//
class Monitor : public EventHandler,
                public std::enable_shared_from_this<Monitor> {
 public:
  using TFunction = std::function<void(Monitor&)>;

  Monitor(std::shared_ptr<Loop> loop, int fd, int event, TFunction fn);

  ~Monitor() override;

  void start();

  void handleEvents(int events) override;

  void cancel();

 private:
  std::shared_ptr<Loop> loop_;
  int fd_;
  int event_;
  TFunction fn_;

  std::mutex mutex_;
  std::condition_variable cv_;
  bool cancelling_{false};
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
  std::shared_ptr<Monitor> monitor(int fd, int event, Monitor::TFunction fn);

 private:
  static constexpr auto capacity_ = 64;

  // Wait for epoll_wait(2) to have returned
  // and handlers to have been executed.
  void waitForLoopTick();

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
  std::condition_variable cv_;

  // Store weak_ptr for every registered fd.
  std::vector<std::weak_ptr<EventHandler>> handlers_;
  std::mutex handlersMutex_;

  // List of functions to run on the next event loop tick.
  std::list<TFunction> deferredFunctions_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
