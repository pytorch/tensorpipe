#include <tensorpipe/transport/shm/loop.h>

#include <sys/eventfd.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

Monitor::Monitor(std::shared_ptr<Loop> loop, int fd, int event, TFunction fn)
    : loop_(std::move(loop)), fd_(fd), event_(event), fn_(std::move(fn)) {
  loop_->registerDescriptor(fd_, event, this);
}

Monitor::~Monitor() {
  cancel();
}

void Monitor::handleEvents(int events) {
  if (events & event_) {
    fn_(*this);
  }
}

// Cancel this event monitor.
//
// This function can be called by any thread, including the event
// loop thread (e.g. from the function called from `handleEvents`).
//
// Take extreme care to make this function re-entrant and that it
// doesn't take a false dependency (e.g. have the event loop thread
// wait on cancellation to complete while some other thread is
// waiting for an event loop tick).
//
void Monitor::cancel() {
  std::unique_lock<std::mutex> lock(mutex_);

  // If this monitor was not yet cancelled, cancel it now.
  if (!cancelled_) {
    if (!cancelling_) {
      cancelling_ = true;

      // Unlock before unregistering because unregistering waits for
      // an event loop tick to finish. If the monitor function calls
      // this function, it must not deadlock.
      lock.unlock();
      loop_->unregisterDescriptor(fd_);

      lock.lock();
      cancelled_ = true;
      lock.unlock();

      cv_.notify_all();
      return;
    }

    // Some other thread started cancellation and may be waiting for
    // an event loop tick. If this is the event loop thread, we
    // cannot block, or we'll block the thread that started
    // cancellation, waiting on a tick in `unregisterDescriptor`.
    if (loop_->isThisTheLoopThread()) {
      return;
    }

    // Wait for cancellation to complete.
    while (!cancelled_) {
      cv_.wait(lock);
    }
  }
}

Loop::Loop() {
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

  // Register for readability on eventfd(2).
  // The user data is left empty here because reading from the
  // eventfd is special cased in the loop's body.
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.ptr = nullptr;
  auto rv = epoll_ctl(epollFd_.fd(), EPOLL_CTL_ADD, eventFd_.fd(), &ev);
  TP_THROW_SYSTEM_IF(rv == -1, errno);

  // Start epoll(2) thread.
  loop_.reset(new std::thread(&Loop::loop, this));
}

Loop::~Loop() {
  if (loop_) {
    done_ = true;
    wakeup();
    loop_->join();
  }
}

void Loop::registerDescriptor(int fd, int events, EventHandler* h) {
  struct epoll_event ev;
  ev.events = events;
  ev.data.ptr = h;

  auto rv = epoll_ctl(epollFd_.fd(), EPOLL_CTL_ADD, fd, &ev);
  if (rv == -1 && errno == EEXIST) {
    rv = epoll_ctl(epollFd_.fd(), EPOLL_CTL_MOD, fd, &ev);
  }
  TP_THROW_SYSTEM_IF(rv == -1, errno);
}

void Loop::unregisterDescriptor(int fd) {
  auto rv = epoll_ctl(epollFd_.fd(), EPOLL_CTL_DEL, fd, nullptr);
  TP_THROW_SYSTEM_IF(rv == -1, errno);

  // Wait for loop to tick before returning, to make sure that the
  // event handler for this fd is guaranteed to not be called once
  // this function returns.
  waitForLoopTick();
}

void Loop::waitForLoopTick() {
  // No need to wait if the event loop thread calls this function,
  // because it means it's not blocking on epoll_wait(2).
  if (std::this_thread::get_id() == loop_->get_id()) {
    return;
  }

  // Wait for loop tick count to change. This means epoll_wait(2)
  // has returned and handlers have been executed.
  std::unique_lock<std::mutex> lock(m_);
  const auto loopTickSnapshot = loopTicks_;
  wakeup();
  while (loopTickSnapshot == loopTicks_) {
    cv_.wait(lock);
  }
}

void Loop::wakeup() {
  // Perform a write to eventfd to wake up epoll_wait(2).
  eventFd_.writeOrThrow<uint64_t>(1);
}

void Loop::loop() {
  std::array<struct epoll_event, capacity_> events;
  while (!done_) {
    auto nfds = epoll_wait(epollFd_.fd(), events.data(), events.size(), -1);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      TP_THROW_SYSTEM(errno);
    }

    // Process events returned by epoll_wait(2).
    for (auto i = 0; i < nfds; i++) {
      const auto& event = events[i];
      auto h = reinterpret_cast<EventHandler*>(event.data.ptr);
      if (h) {
        h->handleEvents(events[i].events);
      } else {
        // Must be a readability event on the eventfd.
        if (event.data.fd == eventFd_.fd()) {
          // Read and discard value from eventfd.
          eventFd_.readOrThrow<uint64_t>();
        }
      }
    }

    // Wake up threads waiting for a loop tick to finish.
    {
      std::unique_lock<std::mutex> lock(m_);
      loopTicks_++;
    }

    cv_.notify_all();
  }
}

bool Loop::isThisTheLoopThread() const {
  return std::this_thread::get_id() == loop_->get_id();
}

std::unique_ptr<Monitor> Loop::monitor(
    int fd,
    int event,
    Monitor::TFunction fn) {
  return std::make_unique<Monitor>(
      shared_from_this(), fd, event, std::move(fn));
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
