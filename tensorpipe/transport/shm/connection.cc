/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/connection.h>

#include <string.h>

#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {
namespace shm {

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
    : loop_(std::move(loop)),
      reactor_(loop_->reactor()),
      socket_(std::move(socket)) {
  // Ensure underlying control socket is non-blocking such that it
  // works well with event driven I/O.
  socket_->block(false);
}

Connection::~Connection() {
  close();
}

void Connection::start() {
  // Create ringbuffer for inbox.
  std::shared_ptr<TRingBuffer> inboxRingBuffer;
  std::tie(inboxHeaderFd_, inboxDataFd_, inboxRingBuffer) =
      util::ringbuffer::shm::create<TRingBuffer>(kDefaultSize);
  inbox_.emplace(std::move(inboxRingBuffer));

  // Register method to be called when our peer writes to our inbox.
  inboxReactorToken_ = reactor_->add(
      runIfAlive(*this, std::function<void(Connection&)>([](Connection& conn) {
        conn.handleInboxReadable();
      })));

  // Register method to be called when our peer reads from our outbox.
  outboxReactorToken_ = reactor_->add(
      runIfAlive(*this, std::function<void(Connection&)>([](Connection& conn) {
        conn.handleOutboxWritable();
      })));

  // We're sending file descriptors first, so wait for writability.
  state_ = SEND_FDS;
  loop_->registerDescriptor(socket_->fd(), EPOLLOUT, shared_from_this());
}

// Implementation of transport::Connection.
void Connection::read(read_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  readOperations_.emplace_back(std::move(fn));

  // If there are pending read operations, make sure the event loop
  // processes them, now that we have an additional callback.
  triggerProcessReadOperations();
}

// Implementation of transport::Connection.
void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  readOperations_.emplace_back(ptr, length, std::move(fn));

  // If there are pending read operations, make sure the event loop
  // processes them, now that we have an additional callback.
  triggerProcessReadOperations();
}

// Implementation of transport::Connection
void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  writeOperations_.emplace_back(ptr, length, std::move(fn));
  triggerProcessWriteOperations();
}

// Implementation of transport::Connection
void Connection::write(
    const google::protobuf::MessageLite& message,
    write_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  writeOperations_.emplace_back(
      [&message](TProducer& outbox) -> ssize_t {
        size_t len = message.ByteSize();
        if (len + sizeof(uint32_t) > kDefaultSize) {
          return -EPERM;
        }

        const auto ret = outbox.writeInTx<uint32_t>(len);
        if (ret < 0) {
          return ret;
        }

        RingBufferZeroCopyOutputStream os(&outbox, len);
        if (!message.SerializeToZeroCopyStream(&os)) {
          return -ENOSPC;
        }

        TP_DCHECK_EQ(len, os.ByteCount());

        return os.ByteCount();
      },
      std::move(fn));
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
  if (state_ == RECV_FDS) {
    Fd reactorHeaderFd;
    Fd reactorDataFd;
    Fd outboxHeaderFd;
    Fd outboxDataFd;
    Reactor::TToken peerInboxReactorToken;
    Reactor::TToken peerOutboxReactorToken;

    // Receive the reactor token, reactor fds, and inbox fds.
    auto err = socket_->recvPayloadAndFds(
        peerInboxReactorToken,
        peerOutboxReactorToken,
        reactorHeaderFd,
        reactorDataFd,
        outboxHeaderFd,
        outboxDataFd);
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    // Load ringbuffer for outbox.
    outbox_.emplace(util::ringbuffer::shm::load<TRingBuffer>(
        outboxHeaderFd.release(), outboxDataFd.release()));

    // Initialize remote reactor trigger.
    peerReactorTrigger_.emplace(
        std::move(reactorHeaderFd), std::move(reactorDataFd));

    peerInboxReactorToken_ = peerInboxReactorToken;
    peerOutboxReactorToken_ = peerOutboxReactorToken;

    // The connection is usable now.
    state_ = ESTABLISHED;
    triggerProcessWriteOperations();
    return;
  }

  if (state_ == ESTABLISHED) {
    // We don't expect to read anything on this socket once the
    // connection has been established. If we do, assume it's a
    // zero-byte read indicating EOF.
    setErrorHoldingMutex(TP_CREATE_ERROR(EOFError));
    closeHoldingMutex();
    processReadOperations(std::move(lock));
    return;
  }

  TP_LOG_WARNING() << "handleEventIn not handled";
}

void Connection::handleEventOut(std::unique_lock<std::mutex> lock) {
  if (state_ == SEND_FDS) {
    int reactorHeaderFd;
    int reactorDataFd;
    std::tie(reactorHeaderFd, reactorDataFd) = reactor_->fds();

    // Send our reactor token, reactor fds, and inbox fds.
    auto err = socket_->sendPayloadAndFds(
        inboxReactorToken_.value(),
        outboxReactorToken_.value(),
        reactorHeaderFd,
        reactorDataFd,
        inboxHeaderFd_,
        inboxDataFd_);
    if (err) {
      failHoldingMutex(std::move(err));
      return;
    }

    // Sent our fds. Wait for fds from peer.
    state_ = RECV_FDS;
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
    return;
  }

  TP_LOG_WARNING() << "handleEventOut not handled";
}

void Connection::handleEventErr(std::unique_lock<std::mutex> lock) {
  setErrorHoldingMutex(TP_CREATE_ERROR(EOFError));
  closeHoldingMutex();
  processReadOperations(std::move(lock));
}

void Connection::handleEventHup(std::unique_lock<std::mutex> lock) {
  setErrorHoldingMutex(TP_CREATE_ERROR(EOFError));
  closeHoldingMutex();
  processReadOperations(std::move(lock));
}

void Connection::handleInboxReadable() {
  std::unique_lock<std::mutex> lock(mutex_);
  processReadOperations(std::move(lock));
}

void Connection::handleOutboxWritable() {
  std::unique_lock<std::mutex> lock(mutex_);
  processWriteOperations(std::move(lock));
}

void Connection::triggerProcessReadOperations() {
  loop_->defer([ptr{shared_from_this()}, this] {
    std::unique_lock<std::mutex> lock(mutex_);
    processReadOperations(std::move(lock));
  });
}

void Connection::processReadOperations(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(lock.owns_lock());

  if (error_) {
    while (!readOperations_.empty()) {
      lock.unlock();
      readOperations_.front().handleError(error_);
      lock.lock();
      readOperations_.pop_front();
    }
    return;
  }

  // Process all read operations that we can immediately serve.
  while (!readOperations_.empty()) {
    auto& readOperation = readOperations_.front();
    lock.unlock();
    if (readOperation.handleRead(*inbox_)) {
      peerReactorTrigger_->run(peerOutboxReactorToken_.value());
    }
    lock.lock();
    if (!readOperation.completed()) {
      break;
    }
    readOperations_.pop_front();
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

  if (error_) {
    while (!writeOperations_.empty()) {
      lock.unlock();
      writeOperations_.front().handleError(error_);
      lock.lock();
      writeOperations_.pop_front();
    }
    return;
  }

  while (!writeOperations_.empty()) {
    auto& writeOperation = writeOperations_.front();
    lock.unlock();
    if (writeOperation.handleWrite(*outbox_)) {
      peerReactorTrigger_->run(peerInboxReactorToken_.value());
    }
    lock.lock();
    if (!writeOperation.completed()) {
      break;
    }
    writeOperations_.pop_front();
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
  if (inboxReactorToken_.has_value()) {
    reactor_->remove(inboxReactorToken_.value());
    inboxReactorToken_.reset();
  }
  if (outboxReactorToken_.has_value()) {
    reactor_->remove(outboxReactorToken_.value());
    outboxReactorToken_.reset();
  }
  if (socket_) {
    loop_->unregisterDescriptor(socket_->fd());
    socket_.reset();
  }
}

Connection::ReadOperation::ReadOperation(
    void* ptr,
    size_t len,
    read_callback_fn fn)
    : ptr_(ptr), len_(len), fn_(std::move(fn)) {}

Connection::ReadOperation::ReadOperation(read_callback_fn fn)
    : fn_(std::move(fn)) {}

bool Connection::ReadOperation::handleRead(TConsumer& inbox) {
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

  bool lengthRead = false;
  if (mode_ == READ_LENGTH) {
    ssize_t ret;
    const uint32_t* lengthPtr;
    std::tie(ret, lengthPtr) = inbox.readInTx<uint32_t>();
    if (ret == -ENODATA) {
      ret = inbox.cancelTx();
      TP_THROW_SYSTEM_IF(ret < 0, -ret);
      return false;
    }
    TP_THROW_SYSTEM_IF(ret < 0, -ret);

    if (ptr_ != nullptr) {
      TP_DCHECK_EQ(*lengthPtr, len_);
    } else {
      len_ = *lengthPtr;
      buf_ = std::make_unique<uint8_t[]>(len_);
      ptr_ = buf_.get();
    }
    mode_ = READ_PAYLOAD;
    lengthRead = true;
  }

  {
    const auto ret = inbox.copyAtMostInTx(
        len_ - bytesRead_, reinterpret_cast<uint8_t*>(ptr_) + bytesRead_);
    if (ret == -ENODATA) {
      if (lengthRead) {
        const auto ret = inbox.commitTx();
        TP_THROW_SYSTEM_IF(ret < 0, -ret);
        return true;
      } else {
        const auto ret = inbox.cancelTx();
        TP_THROW_SYSTEM_IF(ret < 0, -ret);
        return false;
      }
    }
    TP_THROW_SYSTEM_IF(ret < 0, -ret);
    bytesRead_ += ret;
  }

  {
    const auto ret = inbox.commitTx();
    TP_THROW_SYSTEM_IF(ret < 0, -ret);
  }

  if (completed()) {
    fn_(Error::kSuccess, ptr_, len_);
  }

  return true;
}

void Connection::ReadOperation::handleError(const Error& error) {
  fn_(error, nullptr, 0);
}

Connection::WriteOperation::WriteOperation(
    const void* ptr,
    size_t len,
    write_callback_fn fn)
    : ptr_(ptr), len_(len), fn_(std::move(fn)) {}

Connection::WriteOperation::WriteOperation(
    write_fn writer,
    write_callback_fn fn)
    : writer_(std::move(writer)), fn_(std::move(fn)) {}

bool Connection::WriteOperation::handleWrite(TProducer& outbox) {
  // Start write transaction.
  // Retry because this must succeed.
  // TODO: fallback if it doesn't.
  for (;;) {
    const auto ret = outbox.startTx();
    TP_DCHECK(ret >= 0 || ret == -EAGAIN);
    if (ret < 0) {
      continue;
    }
    break;
  }

  ssize_t ret;
  if (writer_) {
    ret = writer_(outbox);
    if (ret > 0) {
      mode_ = WRITE_PAYLOAD;
      bytesWritten_ = len_ = ret;
    }
  } else {
    if (mode_ == WRITE_LENGTH) {
      ret = outbox.writeInTx<uint32_t>(len_);
      if (ret > 0) {
        mode_ = WRITE_PAYLOAD;
      }
    }
    if (mode_ == WRITE_PAYLOAD) {
      ret = outbox.writeAtMostInTx(
          len_ - bytesWritten_,
          static_cast<const uint8_t*>(ptr_) + bytesWritten_);
      if (ret > 0) {
        bytesWritten_ += ret;
      }
    }
  }

  if (ret == -ENOSPC) {
    const auto ret = outbox.cancelTx();
    TP_THROW_SYSTEM_IF(ret < 0, -ret);
    return false;
  }
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  {
    const auto ret = outbox.commitTx();
    TP_THROW_SYSTEM_IF(ret < 0, -ret);
  }

  if (completed()) {
    fn_(Error::kSuccess);
  }

  return true;
}

void Connection::WriteOperation::handleError(const Error& error) {
  fn_(error);
}

Connection::RingBufferZeroCopyOutputStream::RingBufferZeroCopyOutputStream(
    TProducer* buffer,
    size_t payloadSize)
    : buffer_(buffer), payloadSize_(payloadSize) {}

bool Connection::RingBufferZeroCopyOutputStream::Next(void** data, int* size) {
  std::tie(*size, *data) =
      buffer_->reserveContiguousInTx(payloadSize_ - bytesCount_);
  if (*size == -ENOSPC) {
    return false;
  }
  TP_THROW_SYSTEM_IF(*size < 0, -*size);

  bytesCount_ += *size;

  return true;
}

void Connection::RingBufferZeroCopyOutputStream::BackUp(int /* unused */) {}

int64_t Connection::RingBufferZeroCopyOutputStream::ByteCount() const {
  return bytesCount_;
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
