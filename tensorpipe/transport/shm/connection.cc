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
#include <tensorpipe/util/ringbuffer/protobuf_streams.h>

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
  std::shared_ptr<util::ringbuffer::RingBuffer> inboxRingBuffer;
  try {
    std::tie(inboxHeaderFd_, inboxDataFd_, inboxRingBuffer) =
        util::ringbuffer::shm::create(kDefaultSize);
  } catch (std::system_error& e) {
    state_ = INITIALIZING_ERROR;
    // Triggers destructor.
    TP_THROW_SYSTEM(errno) << "Error while creating shm with " << kDefaultSize
                           << " bytes";
  }
  inbox_.emplace(std::move(inboxRingBuffer));

  // Register method to be called when our peer writes to our inbox.
  inboxReactorToken_ = reactor_->add(
      runIfAlive(*this, std::function<void(Connection&)>([](Connection& conn) {
        conn.handleInboxReadableFromReactor();
      })));

  // Register method to be called when our peer reads from our outbox.
  outboxReactorToken_ = reactor_->add(
      runIfAlive(*this, std::function<void(Connection&)>([](Connection& conn) {
        conn.handleOutboxWritableFromReactor();
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
void Connection::read(
    google::protobuf::MessageLite& message,
    read_proto_callback_fn fn) {
  std::unique_lock<std::mutex> guard(mutex_);
  readOperations_.emplace_back(
      [&message](util::ringbuffer::Consumer& inbox) -> ssize_t {
        uint32_t len;
        {
          const auto ret = inbox.copyInTx(sizeof(len), &len);
          if (ret == -ENODATA) {
            return -ENODATA;
          }
          TP_THROW_SYSTEM_IF(ret < 0, -ret);
        }

        if (len + sizeof(uint32_t) > kDefaultSize) {
          return -EPERM;
        }

        util::ringbuffer::ZeroCopyInputStream is(&inbox, len);
        if (!message.ParseFromZeroCopyStream(&is)) {
          return -ENODATA;
        }

        TP_DCHECK_EQ(len, is.ByteCount());
        return is.ByteCount();
      },
      [fn{std::move(fn)}](
          const Error& error, const void* /* unused */, size_t /* unused */) {
        fn(error);
      });

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
      [&message](util::ringbuffer::Producer& outbox) -> ssize_t {
        size_t len = message.ByteSize();
        if (len + sizeof(uint32_t) > kDefaultSize) {
          return -EPERM;
        }

        const auto ret = outbox.writeInTx<uint32_t>(len);
        if (ret < 0) {
          return ret;
        }

        util::ringbuffer::ZeroCopyOutputStream os(&outbox, len);
        if (!message.SerializeToZeroCopyStream(&os)) {
          return -ENOSPC;
        }

        TP_DCHECK_EQ(len, os.ByteCount());

        return os.ByteCount();
      },
      std::move(fn));
  triggerProcessWriteOperations();
}

void Connection::handleEventsFromReactor(int events) {
  TP_DCHECK(loop_->inReactorThread());
  std::unique_lock<std::mutex> lock(mutex_);

  // Handle only one of the events in the mask. Events on the control
  // file descriptor are rare enough for the cost of having epoll call
  // into this function multiple times to not matter. The benefit is
  // that we never have to acquire the lock more than once and that
  // every handler can close and unregister the control file
  // descriptor from the event loop, without worrying about the next
  // handler trying to do so as well.
  if (events & EPOLLIN) {
    handleEventInFromReactor(std::move(lock));
    return;
  }
  if (events & EPOLLOUT) {
    handleEventOutFromReactor(std::move(lock));
    return;
  }
  if (events & EPOLLERR) {
    handleEventErrFromReactor(std::move(lock));
    return;
  }
  if (events & EPOLLHUP) {
    handleEventHupFromReactor(std::move(lock));
    return;
  }
}

void Connection::handleEventInFromReactor(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(loop_->inReactorThread());
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
      failHoldingMutexFromReactor(std::move(err), lock);
      return;
    }

    // Load ringbuffer for outbox.
    outbox_.emplace(util::ringbuffer::shm::load(
        outboxHeaderFd.release(), outboxDataFd.release()));

    // Initialize remote reactor trigger.
    peerReactorTrigger_.emplace(
        std::move(reactorHeaderFd), std::move(reactorDataFd));

    peerInboxReactorToken_ = peerInboxReactorToken;
    peerOutboxReactorToken_ = peerOutboxReactorToken;

    // The connection is usable now.
    state_ = ESTABLISHED;
    processWriteOperationsFromReactor(lock);
    // Trigger read operations in case a pair of local read() and remote
    // write() happened before connection is established. Otherwise read()
    // callback would lose if it's the only read() request.
    processReadOperationsFromReactor(lock);
    return;
  }

  if (state_ == ESTABLISHED) {
    // We don't expect to read anything on this socket once the
    // connection has been established. If we do, assume it's a
    // zero-byte read indicating EOF.
    setErrorHoldingMutexFromReactor(TP_CREATE_ERROR(EOFError));
    closeHoldingMutex();
    processReadOperationsFromReactor(lock);
    return;
  }

  TP_LOG_WARNING() << "handleEventIn not handled";
}

void Connection::handleEventOutFromReactor(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(loop_->inReactorThread());
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
      failHoldingMutexFromReactor(std::move(err), lock);
      return;
    }

    // Sent our fds. Wait for fds from peer.
    state_ = RECV_FDS;
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
    return;
  }

  TP_LOG_WARNING() << "handleEventOut not handled";
}

void Connection::handleEventErrFromReactor(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(loop_->inReactorThread());
  setErrorHoldingMutexFromReactor(TP_CREATE_ERROR(EOFError));
  closeHoldingMutex();
  processReadOperationsFromReactor(lock);
}

void Connection::handleEventHupFromReactor(std::unique_lock<std::mutex> lock) {
  TP_DCHECK(loop_->inReactorThread());
  setErrorHoldingMutexFromReactor(TP_CREATE_ERROR(EOFError));
  closeHoldingMutex();
  processReadOperationsFromReactor(lock);
}

void Connection::handleInboxReadableFromReactor() {
  TP_DCHECK(loop_->inReactorThread());
  std::unique_lock<std::mutex> lock(mutex_);
  processReadOperationsFromReactor(lock);
}

void Connection::handleOutboxWritableFromReactor() {
  TP_DCHECK(loop_->inReactorThread());
  std::unique_lock<std::mutex> lock(mutex_);
  processWriteOperationsFromReactor(lock);
}

void Connection::triggerProcessReadOperations() {
  loop_->deferToReactor([ptr{shared_from_this()}, this] {
    std::unique_lock<std::mutex> lock(mutex_);
    processReadOperationsFromReactor(lock);
  });
}

void Connection::processReadOperationsFromReactor(
    std::unique_lock<std::mutex>& lock) {
  TP_DCHECK(loop_->inReactorThread());
  TP_DCHECK(lock.owns_lock());

  if (error_) {
    std::deque<ReadOperation> operationsToError;
    std::swap(operationsToError, readOperations_);
    lock.unlock();
    for (auto& readOperation : operationsToError) {
      readOperation.handleError(error_);
    }
    lock.lock();
    return;
  }

  // Process all read read operations that we can immediately serve, only
  // when connection is established.
  if (state_ != ESTABLISHED) {
    return;
  }
  // Serve read operations
  while (!readOperations_.empty()) {
    auto readOperation = std::move(readOperations_.front());
    readOperations_.pop_front();
    lock.unlock();
    if (readOperation.handleRead(*inbox_)) {
      peerReactorTrigger_->run(peerOutboxReactorToken_.value());
    }
    lock.lock();
    if (!readOperation.completed()) {
      readOperations_.push_front(std::move(readOperation));
      break;
    }
  }

  TP_DCHECK(lock.owns_lock());
}

void Connection::triggerProcessWriteOperations() {
  loop_->deferToReactor([ptr{shared_from_this()}, this] {
    std::unique_lock<std::mutex> lock(mutex_);
    processWriteOperationsFromReactor(lock);
  });
}

void Connection::processWriteOperationsFromReactor(
    std::unique_lock<std::mutex>& lock) {
  TP_DCHECK(loop_->inReactorThread());
  TP_DCHECK(lock.owns_lock());

  if (state_ < ESTABLISHED) {
    return;
  }

  if (error_) {
    std::deque<WriteOperation> operationsToError;
    std::swap(operationsToError, writeOperations_);
    lock.unlock();
    for (auto& writeOperation : operationsToError) {
      writeOperation.handleError(error_);
    }
    lock.lock();
    return;
  }

  while (!writeOperations_.empty()) {
    auto writeOperation = std::move(writeOperations_.front());
    writeOperations_.pop_front();
    lock.unlock();
    if (writeOperation.handleWrite(*outbox_)) {
      peerReactorTrigger_->run(peerInboxReactorToken_.value());
    }
    lock.lock();
    if (!writeOperation.completed()) {
      writeOperations_.push_front(writeOperation);
      break;
    }
  }

  TP_DCHECK(lock.owns_lock());
}

void Connection::setErrorHoldingMutexFromReactor(Error&& error) {
  TP_DCHECK(loop_->inReactorThread());
  error_ = error;
}

void Connection::failHoldingMutexFromReactor(
    Error&& error,
    std::unique_lock<std::mutex>& lock) {
  TP_DCHECK(loop_->inReactorThread());
  setErrorHoldingMutexFromReactor(std::move(error));
  while (!readOperations_.empty()) {
    auto& readOperation = readOperations_.front();
    lock.unlock();
    readOperation.handleError(error_);
    lock.lock();
    readOperations_.pop_front();
  }
  while (!writeOperations_.empty()) {
    auto& writeOperation = writeOperations_.front();
    lock.unlock();
    writeOperation.handleError(error_);
    lock.lock();
    writeOperations_.pop_front();
  }
}

void Connection::close() {
  // To avoid races, the close operation should also be queued and deferred to
  // the reactor. However, since close can be called from the destructor, we
  // can't extend its lifetime by capturing a shared_ptr and increasing its
  // refcount.
  std::unique_lock<std::mutex> guard(mutex_);
  if (state_ == INITIALIZING_ERROR) {
    return;
  }
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
    : ptr_(ptr), len_(len), fn_(std::move(fn)), ptrProvided_(true) {}

Connection::ReadOperation::ReadOperation(read_fn reader, read_callback_fn fn)
    : reader_(std::move(reader)), fn_(std::move(fn)), ptrProvided_(false) {}

Connection::ReadOperation::ReadOperation(read_callback_fn fn)
    : fn_(std::move(fn)), ptrProvided_(false) {}

bool Connection::ReadOperation::handleRead(util::ringbuffer::Consumer& inbox) {
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
  if (reader_) {
    auto ret = reader_(inbox);
    if (ret == -ENODATA) {
      ret = inbox.cancelTx();
      TP_THROW_SYSTEM_IF(ret < 0, -ret);
      return false;
    }
    TP_THROW_SYSTEM_IF(ret < 0, -ret);

    mode_ = READ_PAYLOAD;
    bytesRead_ = len_ = ret;
  } else {
    if (mode_ == READ_LENGTH) {
      uint32_t length;
      {
        ssize_t ret;
        ret = inbox.copyInTx(sizeof(length), &length);
        if (ret == -ENODATA) {
          ret = inbox.cancelTx();
          TP_THROW_SYSTEM_IF(ret < 0, -ret);
          return false;
        }
        TP_THROW_SYSTEM_IF(ret < 0, -ret);
      }

      if (ptrProvided_) {
        TP_DCHECK_EQ(length, len_);
      } else {
        len_ = length;
        buf_ = std::make_unique<uint8_t[]>(len_);
        ptr_ = buf_.get();
      }
      mode_ = READ_PAYLOAD;
      lengthRead = true;
    }

    // If reading empty buffer, skip payload read.
    if (len_ > 0) {
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

bool Connection::WriteOperation::handleWrite(
    util::ringbuffer::Producer& outbox) {
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

    // If writing empty buffer, skip payload write because ptr_
    // could be nullptr.
    if (mode_ == WRITE_PAYLOAD && len_ > 0) {
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

} // namespace shm
} // namespace transport
} // namespace tensorpipe
