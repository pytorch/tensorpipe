#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

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
class Monitor : public EventHandler {
 public:
  using TFunction = std::function<void(Monitor&)>;

  Monitor(std::shared_ptr<Loop> loop, int fd, int event, TFunction fn);

  ~Monitor() override;

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
  explicit Loop();

  ~Loop();

  void registerDescriptor(int fd, int events, EventHandler* h);

  void unregisterDescriptor(int fd);

  void run();

  // Returns if the calling thread is the same as the loop thread.
  bool isThisTheLoopThread() const;

  // Instantiates an event monitor for the specified fd.
  std::unique_ptr<Monitor> monitor(int fd, int event, Monitor::TFunction fn);

 private:
  static constexpr auto capacity_ = 64;

  // Wait for epoll_wait(2) to have returned
  // and handlers to have been executed.
  void waitForLoopTick();

  // Wake up the event loop.
  void wakeup();

  Fd epollFd_;
  Fd eventFd_;
  uint64_t loopTicks_{0};
  std::atomic<bool> done_{false};
  std::unique_ptr<std::thread> loop_;

  std::mutex m_;
  std::condition_variable cv_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
