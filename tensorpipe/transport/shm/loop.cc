#include <tensorpipe/transport/shm/loop.h>

#include <sys/eventfd.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

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
  loop_.reset(new std::thread(&Loop::run, this));
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
  eventFd_.write<uint64_t>(1);
}

void Loop::run() {
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
          eventFd_.read<uint64_t>();
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

} // namespace shm
} // namespace transport
} // namespace tensorpipe
