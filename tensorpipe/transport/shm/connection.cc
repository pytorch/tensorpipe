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

std::shared_ptr<Connection> Connection::create(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<Socket> socket) {
  auto conn = std::make_shared<Connection>(
      ConstructorToken(), std::move(loop), std::move(socket));
  conn->start();
  return conn;
}

Connection::Connection(
    ConstructorToken /* unused */,
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
}

Connection::~Connection() {
  close();
}

void Connection::start() {
  // We're going to send the eventfd first, so wait for writability.
  state_ = SEND_EVENTFD;
  loop_->registerDescriptor(socket_->fd(), EPOLLOUT, shared_from_this());
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
  std::unique_lock<std::mutex> lock(mutex_);

  // Handle only one of the events in the mask. Events on the control
  // file descriptor are rare enough for the cost of having epoll call
  // into this function multiple times to not matter. The benefit is
  // that we never have to acquire the lock more than once and that
  // every handler can close and unregister the control file
  // descriptor from the event loop, without worrying about the next
  // handler trying to do so as well.
  if (events & EPOLLIN) {
    handleEventIn(std::move(lock));
    return;
  }
  if (events & EPOLLOUT) {
    handleEventOut(std::move(lock));
    return;
  }
  if (events & EPOLLERR) {
    handleEventErr(std::move(lock));
    return;
  }
  if (events & EPOLLHUP) {
    handleEventHup(std::move(lock));
    return;
  }
}

void Connection::handleEventIn(std::unique_lock<std::mutex> lock) {
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
    loop_->registerDescriptor(socket_->fd(), EPOLLOUT, shared_from_this());
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
    inboxMonitor_ = loop_->monitor<Connection>(
        shared_from_this(),
        inboxEventFd_.fd(),
        EPOLLIN,
        [](Connection& conn, FunctionEventHandler& /* unused */) {
          conn.handleInboxReadable();
        });

    // The connection is usable now.
    state_ = ESTABLISHED;
    triggerProcessWriteOperations();
    return;
  }

  if (state_ == ESTABLISHED) {
    // We don't expect to read anything on this socket once the
    // connection has been established. If we do, assume it's a
    // zero-byte read indicating EOF. But, before we fail pending
    // operations, see if there is anything in the inbox.
    readInboxEventFd();
    setErrorHoldingMutex(TP_CREATE_ERROR(EOFError));
    closeHoldingMutex();
    processReadOperations(std::move(lock));
    return;
  }

  TP_LOG_WARNING() << "handleEventIn not handled";
}

void Connection::handleEventOut(std::unique_lock<std::mutex> lock) {
  if (state_ == SEND_EVENTFD) {
    auto err = socket_->sendFd(inboxEventFd_);
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    // Sent our eventfd. Wait for eventfd from peer.
    state_ = RECV_EVENTFD;
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
    return;
  }

  if (state_ == SEND_SEGMENT_PREFIX) {
    auto err = socket_->write(SegmentPrefix(inboxSegmentPrefix_));
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    state_ = RECV_SEGMENT_PREFIX;
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
    return;
  }

  TP_LOG_WARNING() << "handleEventOut not handled";
}

void Connection::handleEventErr(std::unique_lock<std::mutex> lock) {
  readInboxEventFd();
  setErrorHoldingMutex(TP_CREATE_ERROR(EOFError));
  closeHoldingMutex();
  processReadOperations(std::move(lock));
}

void Connection::handleEventHup(std::unique_lock<std::mutex> lock) {
  readInboxEventFd();
  setErrorHoldingMutex(TP_CREATE_ERROR(EOFError));
  closeHoldingMutex();
  processReadOperations(std::move(lock));
}

void Connection::handleInboxReadable() {
  std::unique_lock<std::mutex> lock(mutex_);
  readInboxEventFd();
  processReadOperations(std::move(lock));
}

void Connection::readInboxEventFd() {
  uint64_t value = 0;
  auto err = inboxEventFd_.read(&value);
  if (err) {
    return;
  }

  readOperationsPending_ += value;
}

void Connection::triggerProcessReadOperations() {
  loop_->defer([ptr{shared_from_this()}, this] {
    std::unique_lock<std::mutex> lock(mutex_);
    processReadOperations(std::move(lock));
  });
}

void Connection::processReadOperations(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(lock.owns_lock());

  // Process all read operations that we can immediately serve.
  std::deque<ReadOperation> operationsToRead;
  while (!readOperations_.empty() && readOperationsPending_) {
    auto& readOperation = readOperations_.front();
    operationsToRead.push_back(std::move(readOperation));
    readOperations_.pop_front();
    readOperationsPending_--;
  }

  // If we're in an error state, process all remaining read operations.
  std::deque<ReadOperation> operationsToError;
  if (error_) {
    std::swap(operationsToError, readOperations_);
  }

  // Release lock so that we can trigger these read operations knowing
  // that they can call into this connection's public API without
  // requiring reentrant locking.
  lock.unlock();

  for (auto& readOperation : operationsToRead) {
    readOperation.handleRead(*inbox_);
  }

  for (auto& readOperation : operationsToError) {
    readOperation.handleError(error_);
  }
}

void Connection::triggerProcessWriteOperations() {
  loop_->defer([ptr{shared_from_this()}, this] {
    std::unique_lock<std::mutex> lock(mutex_);
    processWriteOperations(std::move(lock));
  });
}

void Connection::processWriteOperations(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(lock.owns_lock());

  if (state_ < ESTABLISHED) {
    return;
  }

  std::deque<WriteOperation> operationsToWrite;
  if (!error_) {
    std::swap(operationsToWrite, writeOperations_);
  }

  std::deque<WriteOperation> operationsToError;
  if (error_) {
    std::swap(operationsToError, writeOperations_);
  }

  // Release lock so that we can execute these write operations
  // knowing that they can call into this connection's public API
  // without requiring reentrant locking.
  lock.unlock();

  for (auto& writeOperation : operationsToWrite) {
    writeOperation.handleWrite(*outbox_);
    outboxEventFd_.writeOrThrow<uint64_t>(1);
  }

  for (auto& writeOperation : operationsToError) {
    writeOperation.handleError(error_);
  }
}

void Connection::setErrorHoldingMutex(Error&& error) {
  error_ = error;
}

void Connection::failHoldingMutex(Error&& error) {
  setErrorHoldingMutex(std::move(error));
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
