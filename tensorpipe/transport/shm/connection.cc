#include <tensorpipe/transport/shm/connection.h>

#include <string.h>
#include <sys/eventfd.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

Connection::Connection(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<Socket> socket)
    : loop_(std::move(loop)), socket_(std::move(socket)) {
  loop_->registerDescriptor(socket_->fd(), EPOLLIN, this);
}

Connection::~Connection() {
  close();
}

// Implementation of transport::Connection.
void Connection::read(read_callback_fn fn) {
  std::lock_guard<std::mutex> guard(mutex_);
  TP_THROW_EINVAL();
}

// Implementation of transport::Connection.
void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  std::lock_guard<std::mutex> guard(mutex_);
  TP_THROW_EINVAL();
}

// Implementation of transport::Connection
void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  std::lock_guard<std::mutex> guard(mutex_);
  TP_THROW_EINVAL();
}

void Connection::handleEvents(int events) {
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock) {
    return;
  }

  // Ignore for now...
  if (events & EPOLLIN) {
    TP_LOG_INFO() << "Got EPOLLIN";
  }
  if (events & EPOLLOUT) {
    TP_LOG_INFO() << "Got EPOLLOUT";
  }
  if (events & EPOLLERR) {
    TP_LOG_INFO() << "Got EPOLLERR";
  }
  if (events & EPOLLHUP) {
    TP_LOG_INFO() << "Got EPOLLHUP";
  }
}

void Connection::close() {
  std::lock_guard<std::mutex> guard(mutex_);
  closeHoldingMutex();
}

void Connection::closeHoldingMutex() {
  if (socket_) {
    loop_->unregisterDescriptor(socket_->fd());
    socket_.reset();
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
