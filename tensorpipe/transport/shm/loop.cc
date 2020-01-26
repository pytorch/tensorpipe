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

void FunctionEventHandler::handleEvents(int events) {
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

  // Start epoll(2) thread.
  loop_.reset(new std::thread(&Loop::loop, this));
}

Loop::~Loop() {
  TP_DCHECK(done_);
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
  }
}

void Loop::defer(std::function<void()> fn) {
  std::unique_lock<std::mutex> lock(m_);
  functions_.push_back(std::move(fn));
  wakeup();
}

void Loop::wakeup() {
  // Perform a write to eventfd to wake up epoll_wait(2).
  eventFd_.writeOrThrow<uint64_t>(1);
}

void Loop::loop() {
  std::array<struct epoll_event, capacity_> events;

  // Monitor eventfd for readability. Always read from the eventfd so
  // that it is no longer readable on the next call to epoll_wait(2).
  // Note: this is allocated on the stack so that we destroy it upon
  // terminating the event loop thread.
  auto wakeupHandler = std::make_shared<FunctionEventHandler>(
      this, eventFd_.fd(), EPOLLIN, [this](FunctionEventHandler& /* unused */) {
        eventFd_.readOrThrow<uint64_t>();
      });
  wakeupHandler->start();

  for (;;) {
    auto nfds = epoll_wait(epollFd_.fd(), events.data(), events.size(), -1);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      TP_THROW_SYSTEM(errno);
    }

    // Process events returned by epoll_wait(2).
    {
      std::unique_lock<std::mutex> lock(handlersMutex_);
      for (auto i = 0; i < nfds; i++) {
        const auto& event = events[i];
        const auto fd = event.data.fd;
        auto h = handlers_[fd].lock();
        if (h) {
          lock.unlock();
          // Trigger callback. Note that the object is kept alive
          // through the shared_ptr that we acquired by locking the
          // weak_ptr in the handlers vector.
          h->handleEvents(events[i].events);
          // Reset the handler shared_ptr before reacquiring the lock.
          // This may trigger destruction of the object.
          h.reset();
          lock.lock();
        }
      }
    }

    // Process deferred functions. Note that we keep continue running
    // until there are no more functions remaining. This is necessary
    // such that we can assert in the block below that if there are no
    // more handlers, we are done.
    {
      std::unique_lock<std::mutex> lock(m_);
      while (!functions_.empty()) {
        decltype(functions_) stackFunctions;
        std::swap(stackFunctions, functions_);
        lock.unlock();

        // Run deferred functions.
        for (auto& fn : stackFunctions) {
          fn();

          // Reset function to destroy resources associated with it
          // before re-acquiring the lock.
          fn = nullptr;
        }

        lock.lock();
      }
    }

    // Return if another thread is waiting in `join` and there is
    // nothing left to be done. The handler count is equal to 1
    // because we're always monitoring the eventfd for wakeups.
    if (done_ && handlerCount_ == 1) {
      return;
    }
  }
}

void Loop::join() {
  done_ = true;
  wakeup();
  loop_->join();
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
