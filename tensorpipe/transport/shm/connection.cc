#include <tensorpipe/transport/shm/connection.h>

#include <string.h>
#include <sys/eventfd.h>

#include <vector>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/error_macros.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

std::string buildSegmentPrefix(const Fd& fd) {
  std::ostringstream ss;
  ss << "tensorpipe/" << getpid() << "/" << fd.fd();
  return ss.str();
}

// Segment prefix wrapper as a trivially copyable struct.
// Used to easily send the segment prefix over our socket
// as a fixed length message.
class SegmentPrefix {
 public:
  /* implicit */ SegmentPrefix() {}

  explicit SegmentPrefix(const std::string& name) {
    TP_ARG_CHECK_LE(name.size(), sizeof(prefix_) - 1);
    std::memset(prefix_, 0, sizeof(prefix_));
    std::memcpy(prefix_, name.c_str(), name.size());
  };

  std::string str() const {
    return std::string(prefix_, strlen(prefix_));
  }

 private:
  char prefix_[256];
};

} // namespace

Connection::Connection(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<Socket> socket)
    : loop_(std::move(loop)), socket_(std::move(socket)) {
  // Ensure underlying control socket is non-blocking such that it
  // works well with event driven I/O.
  socket_->block(false);

  // Create eventfd(2) for inbox.
  auto fd = eventfd(0, EFD_NONBLOCK);
  TP_THROW_SYSTEM_IF(fd == -1, errno);
  inboxEventFd_ = Fd(fd);

  // Create ringbuffer for inbox.
  inboxSegmentPrefix_ = buildSegmentPrefix(inboxEventFd_);
  inbox_.emplace(util::ringbuffer::shm::create<TRingBuffer>(
      inboxSegmentPrefix_, kDefaultSize, false));

  // We're going to send the eventfd first, so wait for writability.
  state_ = SEND_EVENTFD;
  loop_->registerDescriptor(socket_->fd(), EPOLLOUT, this);
}

Connection::~Connection() {
  close();
}

// Implementation of transport::Connection.
void Connection::read(read_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  readOperations_.emplace_back(std::move(fn));

  // If there are pending read operations, make sure the event loop
  // processes them, now that we have an additional callback. If
  // `readPendingOperations_ == 0`, we'll have to wait for a new read
  // to be signaled, and don't need to force processing.
  if (readOperationsPending_ > 0 || error_) {
    triggerProcessReadOperations();
  }
}

// Implementation of transport::Connection.
void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  TP_THROW_EINVAL();
}

// Implementation of transport::Connection
void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  writeOperations_.emplace_back(ptr, length, std::move(fn));
  triggerProcessWriteOperations();
}

void Connection::handleEvents(int events) {
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock) {
    return;
  }

  if ((events & EPOLLIN) && socket_) {
    handleEventIn();
  }
  if ((events & EPOLLOUT) && socket_) {
    handleEventOut();
  }
  if ((events & EPOLLERR) && socket_) {
    handleEventErr();
  }
  if ((events & EPOLLHUP) && socket_) {
    handleEventHup();
  }
}

void Connection::handleEventIn() {
  if (state_ == RECV_EVENTFD) {
    optional<Fd> fd;
    auto err = socket_->recvFd(&fd);
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    // Successfully received our peer's eventfd.
    outboxEventFd_ = std::move(fd.value());

    // Start ringbuffer prefix exchange.
    state_ = SEND_SEGMENT_PREFIX;
    loop_->registerDescriptor(socket_->fd(), EPOLLOUT, this);
    return;
  }

  if (state_ == RECV_SEGMENT_PREFIX) {
    SegmentPrefix segmentPrefix;
    auto err = socket_->read(&segmentPrefix);
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    // Load ringbuffer for outbox.
    outboxSegmentPrefix_ = segmentPrefix.str();
    outbox_.emplace(
        util::ringbuffer::shm::load<TRingBuffer>(outboxSegmentPrefix_));

    // Monitor eventfd of inbox for reads.
    // If it is readable, it means our peer placed a message in our
    // inbox ringbuffer and is waking us up to process it.
    inboxMonitor_ =
        loop_->monitor(inboxEventFd_.fd(), EPOLLIN, [this](Monitor& monitor) {
          handleInboxReadable();
        });

    // The connection is usable now.
    state_ = ESTABLISHED;
    triggerProcessWriteOperations();
    return;
  }

  if (state_ == ESTABLISHED) {
    // We don't expect to read anything on this socket once the
    // connection has been established. If we do, assume it's a
    // zero-byte read indicating EOF.
    failHoldingMutex(TP_CREATE_ERROR(EOFError));
    return;
  }

  TP_LOG_WARNING() << "handleEventIn not handled";
}

void Connection::handleEventOut() {
  if (state_ == SEND_EVENTFD) {
    auto err = socket_->sendFd(inboxEventFd_);
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    // Sent our eventfd. Wait for eventfd from peer.
    state_ = RECV_EVENTFD;
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, this);
    return;
  }

  if (state_ == SEND_SEGMENT_PREFIX) {
    auto err = socket_->write(SegmentPrefix(inboxSegmentPrefix_));
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    state_ = RECV_SEGMENT_PREFIX;
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, this);
    return;
  }

  TP_LOG_WARNING() << "handleEventOut not handled";
}

void Connection::handleEventErr() {
  closeHoldingMutex();
}

void Connection::handleEventHup() {
  closeHoldingMutex();
}

void Connection::handleInboxReadable() {
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock) {
    return;
  }

  const auto value = inboxEventFd_.readOrThrow<uint64_t>();
  readOperationsPending_ += value;
  processReadOperations(std::move(lock));
}

void Connection::triggerProcessReadOperations() {
  loop_->run([&] {
    std::unique_lock<std::mutex> lock(mutex_);
    processReadOperations(std::move(lock));
  });
}

void Connection::processReadOperations(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(lock.owns_lock());

  // Forward call if we're in an error state.
  if (error_) {
    processReadOperationsInErrorState(std::move(lock));
    return;
  }

  // If we're in an operational state, trigger once for every pending
  // read operation, given there are enough registered read
  // operations.
  std::vector<ReadOperation> localReadOperations;
  localReadOperations.reserve(
      std::min(readOperations_.size(), readOperationsPending_));
  while (!readOperations_.empty() && readOperationsPending_) {
    auto& readOperation = readOperations_.front();
    localReadOperations.push_back(std::move(readOperation));
    readOperations_.pop_front();
    readOperationsPending_--;
  }

  // Release lock so that we can trigger these read operations knowing
  // that they can call into this connection's public API without
  // requiring reentrant locking.
  lock.unlock();
  for (auto& readOperation : localReadOperations) {
    readOperation.handleRead(*inbox_);
  }
}

void Connection::processReadOperationsInErrorState(
    std::unique_lock<std::mutex> lock) {
  TP_DCHECK(lock.owns_lock());
  TP_DCHECK(error_);

  // If we're in an error state, trigger all remaining operations.
  decltype(readOperations_) localReadOperations;
  std::swap(localReadOperations, readOperations_);

  // Release lock so that we can execute these read operations knowing
  // that they can call into this connection's public API without
  // requiring reentrant locking.
  lock.unlock();
  for (auto& readOperation : localReadOperations) {
    readOperation.handleError(error_);
  }
}

void Connection::triggerProcessWriteOperations() {
  loop_->run([&] {
    std::unique_lock<std::mutex> lock(mutex_);
    processWriteOperations(std::move(lock));
  });
}

void Connection::processWriteOperations(std::unique_lock<std::mutex> lock) {
  if (state_ < ESTABLISHED) {
    return;
  }

  // Forward call if we're in an error state.
  if (error_) {
    processWriteOperationsInErrorState(std::move(lock));
    return;
  }

  decltype(writeOperations_) localWriteOperations;
  std::swap(localWriteOperations, writeOperations_);

  // Release lock so that we can execute these write operations
  // knowing that they can call into this connection's public API
  // without requiring reentrant locking.
  lock.unlock();
  for (auto& writeOperation : localWriteOperations) {
    writeOperation.handleWrite(*outbox_);
    outboxEventFd_.writeOrThrow<uint64_t>(1);
  }
}

void Connection::processWriteOperationsInErrorState(
    std::unique_lock<std::mutex> lock) {
  TP_DCHECK(lock.owns_lock());
  TP_DCHECK(error_);

  decltype(writeOperations_) localWriteOperations;
  std::swap(localWriteOperations, writeOperations_);

  // Release lock so that we can execute these write operations
  // knowing that they can call into this connection's public API
  // without requiring reentrant locking.
  lock.unlock();
  for (auto& writeOperation : localWriteOperations) {
    writeOperation.handleError(error_);
  }
}

void Connection::failHoldingMutex(Error&& error) {
  error_ = error;
  while (!readOperations_.empty()) {
    auto& readOperation = readOperations_.front();
    readOperation.handleError(error_);
    readOperations_.pop_front();
  }
  while (!writeOperations_.empty()) {
    auto& writeOperation = writeOperations_.front();
    writeOperation.handleError(error_);
    writeOperations_.pop_front();
  }
}

void Connection::close() {
  std::unique_lock<std::mutex> guard(mutex_);
  closeHoldingMutex();
}

void Connection::closeHoldingMutex() {
  if (socket_) {
    loop_->unregisterDescriptor(socket_->fd());
    socket_.reset();
  }
  if (inboxMonitor_) {
    inboxMonitor_->cancel();
    inboxMonitor_.reset();
  }
}

Connection::ReadOperation::ReadOperation(read_callback_fn fn)
    : fn_(std::move(fn)) {}

void Connection::ReadOperation::handleRead(TConsumer& inbox) {
  // Start read transaction.
  // Retry because this must succeed.
  for (;;) {
    const auto ret = inbox.startTx();
    TP_DCHECK(ret >= 0 || ret == -EAGAIN);
    if (ret < 0) {
      continue;
    }
    break;
  }

  {
    const auto tup = inbox.readInTxWithSize<uint32_t>();
    const auto ret = std::get<0>(tup);
    const auto ptr = std::get<1>(tup);
    TP_THROW_SYSTEM_IF(ret < 0, -ret);
    fn_(Error::kSuccess, ptr, ret);
  }

  {
    const auto ret = inbox.commitTx();
    TP_THROW_SYSTEM_IF(ret < 0, -ret);
  }
}

void Connection::ReadOperation::handleError(const Error& error) {
  fn_(error, nullptr, 0);
}

Connection::WriteOperation::WriteOperation(
    const void* ptr,
    size_t len,
    write_callback_fn fn)
    : ptr_(ptr), len_(len), fn_(std::move(fn)) {}

void Connection::WriteOperation::handleWrite(TProducer& outbox) {
  ssize_t ret;

  // Start write transaction.
  // Retry because this must succeed.
  // TODO: fallback if it doesn't.
  for (;;) {
    ret = outbox.startTx();
    TP_DCHECK(ret >= 0 || ret == -EAGAIN);
    if (ret < 0) {
      continue;
    }
    break;
  }

  ret = outbox.writeInTxWithSize<uint32_t>(len_, ptr_);
  TP_THROW_SYSTEM_IF(ret < 0, -ret);
  ret = outbox.commitTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  fn_(Error::kSuccess);
}

void Connection::WriteOperation::handleError(const Error& error) {
  fn_(error);
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
