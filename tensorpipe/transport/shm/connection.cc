/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/connection.h>

#include <string.h>

#include <deque>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/reactor.h>
#include <tensorpipe/transport/shm/socket.h>
#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>
#include <tensorpipe/util/ringbuffer/protobuf_streams.h>
#include <tensorpipe/util/ringbuffer/shm.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

// Reads happen only if the user supplied a callback (and optionally
// a destination buffer). The callback is run from the event loop
// thread upon receiving a notification from our peer.
//
// The memory pointer argument to the callback is valid only for the
// duration of the callback. If the memory contents must be
// preserved for longer, it must be copied elsewhere.
//
class ReadOperation {
  enum Mode {
    READ_LENGTH,
    READ_PAYLOAD,
  };

 public:
  using read_callback_fn = Connection::read_callback_fn;
  using read_fn = std::function<ssize_t(util::ringbuffer::Consumer&)>;
  explicit ReadOperation(void* ptr, size_t len, read_callback_fn fn);
  explicit ReadOperation(read_fn reader, read_callback_fn fn);
  explicit ReadOperation(read_callback_fn fn);

  // Processes a pending read.
  bool handleRead(util::ringbuffer::Consumer& consumer);

  bool completed() const {
    return (mode_ == READ_PAYLOAD && bytesRead_ == len_);
  }

  void handleError(const Error& error);

 private:
  Mode mode_{READ_LENGTH};
  void* ptr_{nullptr};
  read_fn reader_;
  std::unique_ptr<uint8_t[]> buf_;
  size_t len_{0};
  size_t bytesRead_{0};
  read_callback_fn fn_;
  const bool ptrProvided_;
};

// Writes happen only if the user supplied a memory pointer, the
// number of bytes to write, and a callback to execute upon
// completion of the write.
//
// The memory pointed to by the pointer may only be reused or freed
// after the callback has been called.
//
class WriteOperation {
  enum Mode {
    WRITE_LENGTH,
    WRITE_PAYLOAD,
  };

 public:
  using write_callback_fn = Connection::write_callback_fn;
  using write_fn = std::function<ssize_t(util::ringbuffer::Producer&)>;
  WriteOperation(const void* ptr, size_t len, write_callback_fn fn);
  WriteOperation(write_fn writer, write_callback_fn fn);

  bool handleWrite(util::ringbuffer::Producer& producer);

  bool completed() const {
    return (mode_ == WRITE_PAYLOAD && bytesWritten_ == len_);
  }

  void handleError(const Error& error);

 private:
  Mode mode_{WRITE_LENGTH};
  const void* ptr_{nullptr};
  write_fn writer_;
  size_t len_{0};
  size_t bytesWritten_{0};
  write_callback_fn fn_;
};

ReadOperation::ReadOperation(void* ptr, size_t len, read_callback_fn fn)
    : ptr_(ptr), len_(len), fn_(std::move(fn)), ptrProvided_(true) {}

ReadOperation::ReadOperation(read_fn reader, read_callback_fn fn)
    : reader_(std::move(reader)), fn_(std::move(fn)), ptrProvided_(false) {}

ReadOperation::ReadOperation(read_callback_fn fn)
    : fn_(std::move(fn)), ptrProvided_(false) {}

bool ReadOperation::handleRead(util::ringbuffer::Consumer& inbox) {
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

void ReadOperation::handleError(const Error& error) {
  fn_(error, nullptr, 0);
}

WriteOperation::WriteOperation(
    const void* ptr,
    size_t len,
    write_callback_fn fn)
    : ptr_(ptr), len_(len), fn_(std::move(fn)) {}

WriteOperation::WriteOperation(write_fn writer, write_callback_fn fn)
    : writer_(std::move(writer)), fn_(std::move(fn)) {}

bool WriteOperation::handleWrite(util::ringbuffer::Producer& outbox) {
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

void WriteOperation::handleError(const Error& error) {
  fn_(error);
}

} // namespace

class Connection::Impl : public std::enable_shared_from_this<Connection::Impl>,
                         public EventHandler {
  static constexpr auto kBufferSize = 2 * 1024 * 1024;

  enum State {
    INITIALIZING = 1,
    SEND_FDS,
    RECV_FDS,
    ESTABLISHED,
    DESTROYING,
  };

 public:
  // Create a connection that is already connected (e.g. from a listener).
  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::shared_ptr<Socket> socket);

  // Create a connection that connects to the specified address.
  Impl(std::shared_ptr<Context::PrivateIface> context, address_t addr);

  // Initialize member fields that need `shared_from_this`.
  void init();

  // Queue a read operation.
  void read(read_callback_fn fn);
  void read(google::protobuf::MessageLite& message, read_proto_callback_fn fn);
  void read(void* ptr, size_t length, read_callback_fn fn);

  // Perform a write operation.
  void write(const void* ptr, size_t length, write_callback_fn fn);
  void write(
      const google::protobuf::MessageLite& message,
      write_callback_fn fn);

  // Shut down the connection and its resources.
  void close();

  // Implementation of EventHandler.
  void handleEventsFromLoop(int events) override;

 private:
  // Initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Queue a read operation.
  void readFromLoop(read_callback_fn fn);
  void readFromLoop(
      google::protobuf::MessageLite& message,
      read_proto_callback_fn fn);
  void readFromLoop(void* ptr, size_t length, read_callback_fn fn);

  // Perform a write operation.
  void writeFromLoop(const void* ptr, size_t length, write_callback_fn fn);
  void writeFromLoop(
      const google::protobuf::MessageLite& message,
      write_callback_fn fn);

  // Shut down the connection and its resources.
  void closeFromLoop();

  // Handle events of type EPOLLIN.
  void handleEventInFromLoop();

  // Handle events of type EPOLLOUT.
  void handleEventOutFromLoop();

  // Handle events of type EPOLLERR.
  void handleEventErrFromLoop();

  // Handle events of type EPOLLHUP.
  void handleEventHupFromLoop();

  // Handle inbox being readable.
  //
  // This is triggered from the reactor loop when this connection's
  // peer has written an entry into our inbox. It is called once per
  // message. Because it's called from another thread, we must always
  // take care to acquire the connection's lock here.
  void handleInboxReadableFromLoop();

  // Handle outbox being writable.
  //
  // This is triggered from the reactor loop when this connection's
  // peer has read an entry from our outbox. It is called once per
  // message. Because it's called from another thread, we must always
  // take care to acquire the connection's lock here.
  void handleOutboxWritableFromLoop();

  State state_{INITIALIZING};
  Error error_;
  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<Socket> socket_;
  optional<Sockaddr> sockaddr_;
  ClosingReceiver closingReceiver_;

  // Inbox.
  int inboxHeaderFd_;
  int inboxDataFd_;
  optional<util::ringbuffer::Consumer> inbox_;
  optional<Reactor::TToken> inboxReactorToken_;

  // Outbox.
  optional<util::ringbuffer::Producer> outbox_;
  optional<Reactor::TToken> outboxReactorToken_;

  // Peer trigger/tokens.
  optional<Reactor::Trigger> peerReactorTrigger_;
  optional<Reactor::TToken> peerInboxReactorToken_;
  optional<Reactor::TToken> peerOutboxReactorToken_;

  // Pending read operations.
  std::deque<ReadOperation> readOperations_;

  // Pending write operations.
  std::deque<WriteOperation> writeOperations_;

  // Defer execution of processReadOperations to loop thread.
  void triggerProcessReadOperations();

  // Process pending read operations if in an operational or error state.
  void processReadOperationsFromLoop();

  // Defer execution of processWriteOperations to loop thread.
  void triggerProcessWriteOperations();

  // Process pending write operations if in an operational state.
  void processWriteOperationsFromLoop();

  // Fail with error.
  void failFromLoop(Error&&);
};

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<Socket> socket)
    : impl_(std::make_shared<Impl>(std::move(context), std::move(socket))) {
  impl_->init();
}

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr)
    : impl_(std::make_shared<Impl>(std::move(context), std::move(addr))) {
  impl_->init();
}

void Connection::Impl::init() {
  context_->deferToLoop([impl{shared_from_this()}]() { impl->initFromLoop(); });
}

void Connection::close() {
  impl_->close();
}

void Connection::Impl::close() {
  context_->deferToLoop(
      [impl{shared_from_this()}]() { impl->closeFromLoop(); });
}

Connection::~Connection() {
  close();
}

Connection::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<Socket> socket)
    : context_(std::move(context)),
      socket_(std::move(socket)),
      closingReceiver_(context_, context_->getClosingEmitter()) {}

Connection::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr)
    : context_(std::move(context)),
      socket_(Socket::createForFamily(AF_UNIX)),
      sockaddr_(Sockaddr::createAbstractUnixAddr(addr)),
      closingReceiver_(context_, context_->getClosingEmitter()) {}

void Connection::Impl::initFromLoop() {
  TP_DCHECK(context_->inLoopThread());

  closingReceiver_.activate(*this);

  if (sockaddr_.has_value()) {
    socket_->connect(sockaddr_.value());
  }
  // Ensure underlying control socket is non-blocking such that it
  // works well with event driven I/O.
  socket_->block(false);

  // Create ringbuffer for inbox.
  std::shared_ptr<util::ringbuffer::RingBuffer> inboxRingBuffer;
  std::tie(inboxHeaderFd_, inboxDataFd_, inboxRingBuffer) =
      util::ringbuffer::shm::create(kBufferSize);
  inbox_.emplace(std::move(inboxRingBuffer));

  // Register method to be called when our peer writes to our inbox.
  inboxReactorToken_ = context_->addReaction(
      runIfAlive(*this, std::function<void(Impl&)>([](Impl& impl) {
        impl.handleInboxReadableFromLoop();
      })));

  // Register method to be called when our peer reads from our outbox.
  outboxReactorToken_ = context_->addReaction(
      runIfAlive(*this, std::function<void(Impl&)>([](Impl& impl) {
        impl.handleOutboxWritableFromLoop();
      })));

  // We're sending file descriptors first, so wait for writability.
  state_ = SEND_FDS;
  context_->registerDescriptor(socket_->fd(), EPOLLOUT, shared_from_this());
}

void Connection::read(read_callback_fn fn) {
  impl_->read(std::move(fn));
}

void Connection::Impl::read(read_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, fn{std::move(fn)}]() mutable {
        impl->readFromLoop(std::move(fn));
      });
}

void Connection::Impl::readFromLoop(read_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  readOperations_.emplace_back(std::move(fn));

  // If there are pending read operations, make sure the event loop
  // processes them, now that we have an additional callback.
  triggerProcessReadOperations();
}

void Connection::read(
    google::protobuf::MessageLite& message,
    read_proto_callback_fn fn) {
  impl_->read(message, std::move(fn));
}

void Connection::Impl::read(
    google::protobuf::MessageLite& message,
    read_proto_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, &message, fn{std::move(fn)}]() mutable {
        impl->readFromLoop(message, std::move(fn));
      });
}

void Connection::Impl::readFromLoop(
    google::protobuf::MessageLite& message,
    read_proto_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

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

        if (len + sizeof(uint32_t) > kBufferSize) {
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

void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  impl_->read(ptr, length, std::move(fn));
}

void Connection::Impl::read(void* ptr, size_t length, read_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, ptr, length, fn{std::move(fn)}]() mutable {
        impl->readFromLoop(ptr, length, std::move(fn));
      });
}

void Connection::Impl::readFromLoop(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  readOperations_.emplace_back(ptr, length, std::move(fn));

  // If there are pending read operations, make sure the event loop
  // processes them, now that we have an additional callback.
  triggerProcessReadOperations();
}

void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  impl_->write(ptr, length, std::move(fn));
}

void Connection::Impl::write(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, ptr, length, fn{std::move(fn)}]() mutable {
        impl->writeFromLoop(ptr, length, std::move(fn));
      });
}

void Connection::Impl::writeFromLoop(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  writeOperations_.emplace_back(ptr, length, std::move(fn));
  triggerProcessWriteOperations();
}

void Connection::write(
    const google::protobuf::MessageLite& message,
    write_callback_fn fn) {
  impl_->write(message, std::move(fn));
}

void Connection::Impl::write(
    const google::protobuf::MessageLite& message,
    write_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, &message, fn{std::move(fn)}]() mutable {
        impl->writeFromLoop(message, std::move(fn));
      });
}

void Connection::Impl::writeFromLoop(
    const google::protobuf::MessageLite& message,
    write_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  writeOperations_.emplace_back(
      [&message](util::ringbuffer::Producer& outbox) -> ssize_t {
        size_t len = message.ByteSize();
        if (len + sizeof(uint32_t) > kBufferSize) {
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

void Connection::Impl::handleEventsFromLoop(int events) {
  TP_DCHECK(context_->inLoopThread());

  // Handle only one of the events in the mask. Events on the control
  // file descriptor are rare enough for the cost of having epoll call
  // into this function multiple times to not matter. The benefit is
  // that every handler can close and unregister the control file
  // descriptor from the event loop, without worrying about the next
  // handler trying to do so as well.
  if (events & EPOLLIN) {
    handleEventInFromLoop();
    return;
  }
  if (events & EPOLLOUT) {
    handleEventOutFromLoop();
    return;
  }
  if (events & EPOLLERR) {
    handleEventErrFromLoop();
    return;
  }
  if (events & EPOLLHUP) {
    handleEventHupFromLoop();
    return;
  }
}

void Connection::Impl::handleEventInFromLoop() {
  TP_DCHECK(context_->inLoopThread());
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
      failFromLoop(std::move(err));
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
    processWriteOperationsFromLoop();
    // Trigger read operations in case a pair of local read() and remote
    // write() happened before connection is established. Otherwise read()
    // callback would lose if it's the only read() request.
    processReadOperationsFromLoop();
    return;
  }

  if (state_ == ESTABLISHED) {
    // We don't expect to read anything on this socket once the
    // connection has been established. If we do, assume it's a
    // zero-byte read indicating EOF.
    error_ = TP_CREATE_ERROR(EOFError);
    closeFromLoop();
    processReadOperationsFromLoop();
    return;
  }

  TP_LOG_WARNING() << "handleEventIn not handled";
}

void Connection::Impl::handleEventOutFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  if (state_ == SEND_FDS) {
    int reactorHeaderFd;
    int reactorDataFd;
    std::tie(reactorHeaderFd, reactorDataFd) = context_->reactorFds();

    // Send our reactor token, reactor fds, and inbox fds.
    auto err = socket_->sendPayloadAndFds(
        inboxReactorToken_.value(),
        outboxReactorToken_.value(),
        reactorHeaderFd,
        reactorDataFd,
        inboxHeaderFd_,
        inboxDataFd_);
    if (err) {
      failFromLoop(std::move(err));
      return;
    }

    // Sent our fds. Wait for fds from peer.
    state_ = RECV_FDS;
    context_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
    return;
  }

  TP_LOG_WARNING() << "handleEventOut not handled";
}

void Connection::Impl::handleEventErrFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  error_ = TP_CREATE_ERROR(EOFError);
  closeFromLoop();
  processReadOperationsFromLoop();
}

void Connection::Impl::handleEventHupFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  error_ = TP_CREATE_ERROR(EOFError);
  closeFromLoop();
  processReadOperationsFromLoop();
}

void Connection::Impl::handleInboxReadableFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  processReadOperationsFromLoop();
}

void Connection::Impl::handleOutboxWritableFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  processWriteOperationsFromLoop();
}

void Connection::Impl::triggerProcessReadOperations() {
  context_->deferToLoop(
      [ptr{shared_from_this()}, this] { processReadOperationsFromLoop(); });
}

void Connection::Impl::processReadOperationsFromLoop() {
  TP_DCHECK(context_->inLoopThread());

  if (error_) {
    std::deque<ReadOperation> operationsToError;
    std::swap(operationsToError, readOperations_);
    for (auto& readOperation : operationsToError) {
      readOperation.handleError(error_);
    }
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
    if (readOperation.handleRead(*inbox_)) {
      peerReactorTrigger_->run(peerOutboxReactorToken_.value());
    }
    if (!readOperation.completed()) {
      readOperations_.push_front(std::move(readOperation));
      break;
    }
  }
}

void Connection::Impl::triggerProcessWriteOperations() {
  context_->deferToLoop(
      [ptr{shared_from_this()}, this] { processWriteOperationsFromLoop(); });
}

void Connection::Impl::processWriteOperationsFromLoop() {
  TP_DCHECK(context_->inLoopThread());

  if (state_ < ESTABLISHED) {
    return;
  }

  if (error_) {
    std::deque<WriteOperation> operationsToError;
    std::swap(operationsToError, writeOperations_);
    for (auto& writeOperation : operationsToError) {
      writeOperation.handleError(error_);
    }
    return;
  }

  while (!writeOperations_.empty()) {
    auto writeOperation = std::move(writeOperations_.front());
    writeOperations_.pop_front();
    if (writeOperation.handleWrite(*outbox_)) {
      peerReactorTrigger_->run(peerInboxReactorToken_.value());
    }
    if (!writeOperation.completed()) {
      writeOperations_.push_front(writeOperation);
      break;
    }
  }
}

void Connection::Impl::failFromLoop(Error&& error) {
  TP_DCHECK(context_->inLoopThread());
  error_ = std::move(error);
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

void Connection::Impl::closeFromLoop() {
  TP_DCHECK(context_->inLoopThread());

  failFromLoop(TP_CREATE_ERROR(ConnectionClosedError));
  if (inboxReactorToken_.has_value()) {
    context_->removeReaction(inboxReactorToken_.value());
    inboxReactorToken_.reset();
  }
  if (outboxReactorToken_.has_value()) {
    context_->removeReaction(outboxReactorToken_.value());
    outboxReactorToken_.reset();
  }
  if (socket_) {
    context_->unregisterDescriptor(socket_->fd());
    socket_.reset();
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
