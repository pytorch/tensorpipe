#pragma once

#include <atomic>
#include <condition_variable>
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

class Loop final {
 public:
  explicit Loop();

  ~Loop();

  void registerDescriptor(int fd, int events, EventHandler* h);

  void unregisterDescriptor(int fd);

  void run();

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
