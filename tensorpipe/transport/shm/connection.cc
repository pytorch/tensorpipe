#include <tensorpipe/transport/shm/connection.h>

#include <string.h>
#include <sys/eventfd.h>

#include <tensorpipe/common/defs.h>

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
  std::lock_guard<std::mutex> guard(mutex_);
  readOperations_.emplace_back(std::move(fn));
  processReadOperationsWhileHoldingLock();
}

// Implementation of transport::Connection.
void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  std::lock_guard<std::mutex> guard(mutex_);
  TP_THROW_EINVAL();
}

// Implementation of transport::Connection
void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  std::lock_guard<std::mutex> guard(mutex_);
  writeOperations_.emplace_back(ptr, length, std::move(fn));
  processWriteOperationsWhileHoldingLock();
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
    auto rv = socket_->recvFd(&fd);
    if (rv == -1) {
      TP_THROW_SYSTEM(errno);
    }

    if (rv == 0) {
      TP_LOG_WARNING() << "Read EOF in RECV_EVENTFD";
      closeHoldingMutex();
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
    auto segmentPrefix = socket_->readOrThrow<SegmentPrefix>();

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
    processWriteOperationsWhileHoldingLock();
    return;
  }

  if (state_ == ESTABLISHED) {
    return;
  }

  TP_LOG_WARNING() << "handleEventIn not handled";
}

void Connection::handleEventOut() {
  if (state_ == SEND_EVENTFD) {
    auto rv = socket_->sendFd(inboxEventFd_);
    if (rv == -1) {
      TP_LOG_WARNING() << "sendFd: " << strerror(errno);
      return;
    }

    // Sent our eventfd. Wait for eventfd from peer.
    state_ = RECV_EVENTFD;
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, this);
    return;
  }

  if (state_ == SEND_SEGMENT_PREFIX) {
    socket_->writeOrThrow(SegmentPrefix(inboxSegmentPrefix_));
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
  processReadOperationsWhileHoldingLock();
}

void Connection::processReadOperationsWhileHoldingLock() {
  // Process read operations in FIFO order.
  while (!readOperations_.empty() && readOperationsPending_) {
    auto& readOperation = readOperations_.front();
    readOperation.handleRead(*inbox_);
    readOperations_.pop_front();
    readOperationsPending_--;
  }
}

void Connection::processWriteOperationsWhileHoldingLock() {
  if (state_ < ESTABLISHED) {
    return;
  }

  // Process write operations in FIFO order.
  while (!writeOperations_.empty()) {
    auto& writeOperation = writeOperations_.front();
    writeOperation.handleWrite(*outbox_);
    writeOperations_.pop_front();
    outboxEventFd_.writeOrThrow<uint64_t>(1);
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
    fn_(Error(), ptr, ret);
  }

  {
    const auto ret = inbox.commitTx();
    TP_THROW_SYSTEM_IF(ret < 0, -ret);
  }
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

  fn_(Error());
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
