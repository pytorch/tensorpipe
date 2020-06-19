/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/connection.h>

#include <array>
#include <deque>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

namespace {

// The read operation captures all state associated with reading a
// fixed length chunk of data from the underlying connection. All
// reads are required to include a word-sized header containing the
// number of bytes in the operation. This makes it possible for the
// read side of the connection to either 1) not know how many bytes
// to expected, and dynamically allocate, or 2) know how many bytes
// to expect, and preallocate the destination memory.
class ReadOperation {
  enum Mode {
    READ_LENGTH,
    READ_PAYLOAD,
    COMPLETE,
  };

 public:
  using read_callback_fn = Connection::read_callback_fn;

  explicit ReadOperation(read_callback_fn fn);

  ReadOperation(void* ptr, size_t length, read_callback_fn fn);

  // Called when libuv is about to read data from connection.
  void allocFromLoop(uv_buf_t* buf);

  // Called when libuv has read data from connection.
  void readFromLoop(ssize_t nread, const uv_buf_t* buf);

  // Returns if this read operation is complete.
  inline bool completeFromLoop() const;

  // Invoke user callback.
  inline void callbackFromLoop(const Error& error);

 private:
  Mode mode_{READ_LENGTH};
  char* ptr_{nullptr};

  // Number of bytes as specified by the user (if applicable).
  optional<size_t> givenLength_;

  // Number of bytes to expect as read from the connection.
  size_t readLength_{0};

  // Number of bytes read from the connection.
  // This is reset to 0 when we advance from READ_LENGTH to READ_PAYLOAD.
  size_t bytesRead_{0};

  // Holds temporary allocation if no length was specified.
  std::unique_ptr<char[]> buffer_{nullptr};

  // User callback.
  read_callback_fn fn_;
};

ReadOperation::ReadOperation(read_callback_fn fn) : fn_(std::move(fn)) {}

ReadOperation::ReadOperation(void* ptr, size_t length, read_callback_fn fn)
    : ptr_(static_cast<char*>(ptr)), givenLength_(length), fn_(std::move(fn)) {}

void ReadOperation::allocFromLoop(uv_buf_t* buf) {
  if (mode_ == READ_LENGTH) {
    TP_DCHECK_LT(bytesRead_, sizeof(readLength_));
    buf->base = reinterpret_cast<char*>(&readLength_) + bytesRead_;
    buf->len = sizeof(readLength_) - bytesRead_;
  } else if (mode_ == READ_PAYLOAD) {
    TP_DCHECK_LT(bytesRead_, readLength_);
    TP_DCHECK(ptr_ != nullptr);
    buf->base = ptr_ + bytesRead_;
    buf->len = readLength_ - bytesRead_;
  } else {
    TP_THROW_ASSERT() << "invalid mode " << mode_;
  }
}

void ReadOperation::readFromLoop(ssize_t nread, const uv_buf_t* buf) {
  TP_DCHECK_GE(nread, 0);
  bytesRead_ += nread;
  if (mode_ == READ_LENGTH) {
    TP_DCHECK_LE(bytesRead_, sizeof(readLength_));
    if (bytesRead_ == sizeof(readLength_)) {
      if (givenLength_.has_value()) {
        TP_DCHECK(ptr_ != nullptr || givenLength_.value() == 0);
        TP_DCHECK_EQ(readLength_, givenLength_.value());
      } else {
        TP_DCHECK(ptr_ == nullptr);
        buffer_ = std::make_unique<char[]>(readLength_);
        ptr_ = buffer_.get();
      }
      if (readLength_ == 0) {
        mode_ = COMPLETE;
      } else {
        mode_ = READ_PAYLOAD;
      }
      bytesRead_ = 0;
    }
  } else if (mode_ == READ_PAYLOAD) {
    TP_DCHECK_LE(bytesRead_, readLength_);
    if (bytesRead_ == readLength_) {
      mode_ = COMPLETE;
    }
  } else {
    TP_THROW_ASSERT() << "invalid mode " << mode_;
  }
}

bool ReadOperation::completeFromLoop() const {
  return mode_ == COMPLETE;
}

void ReadOperation::callbackFromLoop(const Error& error) {
  fn_(error, ptr_, readLength_);
}

// The write operation captures all state associated with writing a
// fixed length chunk of data from the underlying connection. The
// write includes a word-sized header containing the length of the
// write. This header is a member field on this class and therefore
// the instance must be kept alive and the reference to the instance
// must remain valid until the write callback has been called.
class WriteOperation {
 public:
  using write_callback_fn = Connection::write_callback_fn;

  WriteOperation(const void* ptr, size_t length, write_callback_fn fn);

  inline std::tuple<uv_buf_t*, unsigned int> getBufs();

  // Invoke user callback.
  inline void callbackFromLoop(const Error& error);

 private:
  const char* ptr_;
  const size_t length_;

  // Buffers (structs with pointers and lengths) we pass to uv_write.
  std::array<uv_buf_t, 2> bufs_;

  // User callback.
  write_callback_fn fn_;
};

WriteOperation::WriteOperation(
    const void* ptr,
    size_t length,
    write_callback_fn fn)
    : ptr_(static_cast<const char*>(ptr)), length_(length), fn_(std::move(fn)) {
  bufs_[0].base = const_cast<char*>(reinterpret_cast<const char*>(&length_));
  bufs_[0].len = sizeof(length_);
  bufs_[1].base = const_cast<char*>(ptr_);
  bufs_[1].len = length_;
}

std::tuple<uv_buf_t*, unsigned int> WriteOperation::getBufs() {
  // Libuv doesn't like when we pass it empty buffers (it fails with ENOBUFS),
  // so if the second buffer is empty we only pass it the first one.
  unsigned int numBuffers = length_ == 0 ? 1 : 2;
  return std::make_tuple(bufs_.data(), numBuffers);
}

void WriteOperation::callbackFromLoop(const Error& error) {
  fn_(error);
}

} // namespace

class Connection::Impl : public std::enable_shared_from_this<Connection::Impl> {
 public:
  // Create a connection that is already connected (e.g. from a listener).
  Impl(
      std::shared_ptr<Context::PrivateIface>,
      std::shared_ptr<TCPHandle>,
      std::string);

  // Create a connection that connects to the specified address.
  Impl(std::shared_ptr<Context::PrivateIface>, address_t, std::string);

  // Initialize member fields that need `shared_from_this`.
  void init();

  // Queue a read operation.
  void read(read_callback_fn fn);
  void read(void* ptr, size_t length, read_callback_fn fn);

  // Perform a write operation.
  void write(const void* ptr, size_t length, write_callback_fn fn);

  // Tell the connection what its identifier is.
  void setId(std::string id);

  // Shut down the connection and its resources.
  void close();

 private:
  // Initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Queue a read operation.
  void readFromLoop(read_callback_fn fn);
  void readFromLoop(void* ptr, size_t length, read_callback_fn fn);

  // Perform a write operation.
  void writeFromLoop(const void* ptr, size_t length, write_callback_fn fn);

  void setIdFromLoop_(std::string id);

  // Shut down the connection and its resources.
  void closeFromLoop();

  // Called when libuv is about to read data from connection.
  void allocCallbackFromLoop_(uv_buf_t* buf);

  // Called when libuv has read data from connection.
  void readCallbackFromLoop_(ssize_t nread, const uv_buf_t* buf);

  // Called when libuv has written data to connection.
  void writeCallbackFromLoop_(int status);

  // Called when libuv has closed the handle.
  void closeCallbackFromLoop_();

  void setError_(Error error);

  // Deal with an error.
  void handleError_();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<TCPHandle> handle_;
  optional<Sockaddr> sockaddr_;
  Error error_{Error::kSuccess};
  ClosingReceiver closingReceiver_;

  std::deque<ReadOperation> readOperations_;
  std::deque<WriteOperation> writeOperations_;

  // A sequence number for the calls to read and write.
  uint64_t nextBufferBeingRead_{0};
  uint64_t nextBufferBeingWritten_{0};

  // A sequence number for the invocations of the callbacks of read and write.
  uint64_t nextReadCallbackToCall_{0};
  uint64_t nextWriteCallbackToCall_{0};

  // An identifier for the connection, composed of the identifier for the
  // context or listener, combined with an increasing sequence number. It will
  // only be used for logging and debugging purposes.
  std::string id_;

  // By having the instance store a shared_ptr to itself we create a reference
  // cycle which will "leak" the instance. This allows us to detach its
  // lifetime from the connection and sync it with the TCPHandle's life cycle.
  std::shared_ptr<Impl> leak_;
};

Connection::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<TCPHandle> handle,
    std::string id)
    : context_(std::move(context)),
      handle_(std::move(handle)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

Connection::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr,
    std::string id)
    : context_(std::move(context)),
      handle_(context_->createHandle()),
      sockaddr_(Sockaddr::createInetSockAddr(addr)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

void Connection::Impl::initFromLoop() {
  leak_ = shared_from_this();

  closingReceiver_.activate(*this);

  if (sockaddr_.has_value()) {
    handle_->initFromLoop();
    handle_->connectFromLoop(sockaddr_.value(), [this](int status) {
      if (status < 0) {
        setError_(TP_CREATE_ERROR(UVError, status));
      }
    });
  }
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop_(); });
  handle_->armAllocCallbackFromLoop(
      [this](uv_buf_t* buf) { this->allocCallbackFromLoop_(buf); });
  handle_->armReadCallbackFromLoop([this](ssize_t nread, const uv_buf_t* buf) {
    this->readCallbackFromLoop_(nread, buf);
  });
}

void Connection::Impl::readFromLoop(read_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  uint64_t sequenceNumber = nextBufferBeingRead_++;
  TP_VLOG(7) << "Connection " << id_ << " received a read request (#"
             << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, const void* ptr, size_t length) {
    TP_DCHECK_EQ(sequenceNumber, nextReadCallbackToCall_++);
    TP_VLOG(7) << "Connection " << id_ << " is calling a read callback (#"
               << sequenceNumber << ")";
    fn(error, ptr, length);
    TP_VLOG(7) << "Connection " << id_ << " done calling a read callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    fn(error_, nullptr, 0);
    return;
  }

  readOperations_.emplace_back(std::move(fn));

  // Start reading if this is the first read operation.
  if (readOperations_.size() == 1) {
    handle_->readStartFromLoop();
  }
}

void Connection::Impl::readFromLoop(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  uint64_t sequenceNumber = nextBufferBeingRead_++;
  TP_VLOG(7) << "Connection " << id_ << " received a read request (#"
             << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, const void* ptr, size_t length) {
    TP_DCHECK_EQ(sequenceNumber, nextReadCallbackToCall_++);
    TP_VLOG(7) << "Connection " << id_ << " is calling a read callback (#"
               << sequenceNumber << ")";
    fn(error, ptr, length);
    TP_VLOG(7) << "Connection " << id_ << " done calling a read callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    fn(error_, ptr, length);
    return;
  }

  readOperations_.emplace_back(ptr, length, std::move(fn));

  // Start reading if this is the first read operation.
  if (readOperations_.size() == 1) {
    handle_->readStartFromLoop();
  }
}

void Connection::Impl::writeFromLoop(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  uint64_t sequenceNumber = nextBufferBeingWritten_++;
  TP_VLOG(7) << "Connection " << id_ << " received a write request (#"
             << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](const Error& error) {
    TP_DCHECK_EQ(sequenceNumber, nextWriteCallbackToCall_++);
    TP_VLOG(7) << "Connection " << id_ << " is calling a write callback (#"
               << sequenceNumber << ")";
    fn(error);
    TP_VLOG(7) << "Connection " << id_ << " done calling a write callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    fn(error_);
    return;
  }

  writeOperations_.emplace_back(ptr, length, std::move(fn));

  auto& writeOperation = writeOperations_.back();
  uv_buf_t* bufsPtr;
  unsigned int bufsLen;
  std::tie(bufsPtr, bufsLen) = writeOperation.getBufs();
  handle_->writeFromLoop(bufsPtr, bufsLen, [this](int status) {
    this->writeCallbackFromLoop_(status);
  });
}

void Connection::Impl::setId(std::string id) {
  context_->deferToLoop(
      [impl{shared_from_this()}, id{std::move(id)}]() mutable {
        impl->setIdFromLoop_(std::move(id));
      });
}

void Connection::Impl::setIdFromLoop_(std::string id) {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(7) << "Connection " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

void Connection::Impl::close() {
  context_->deferToLoop(
      [impl{shared_from_this()}]() { impl->closeFromLoop(); });
}

void Connection::Impl::closeFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(7) << "Connection " << id_ << " is closing";
  setError_(TP_CREATE_ERROR(ConnectionClosedError));
}

void Connection::Impl::allocCallbackFromLoop_(uv_buf_t* buf) {
  TP_DCHECK(context_->inLoopThread());
  TP_THROW_ASSERT_IF(readOperations_.empty());
  TP_VLOG(9) << "Connection " << id_
             << " has incoming data for which it needs to provide a buffer";
  readOperations_.front().allocFromLoop(buf);
}

void Connection::Impl::readCallbackFromLoop_(
    ssize_t nread,
    const uv_buf_t* buf) {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(9) << "Connection " << id_ << " has completed reading some data ("
             << (nread >= 0 ? std::to_string(nread) + " bytes"
                            : formatUvError(nread))
             << ")";

  if (nread < 0) {
    setError_(TP_CREATE_ERROR(UVError, nread));
    return;
  }

  TP_THROW_ASSERT_IF(readOperations_.empty());
  auto& readOperation = readOperations_.front();
  readOperation.readFromLoop(nread, buf);
  if (readOperation.completeFromLoop()) {
    readOperation.callbackFromLoop(Error::kSuccess);
    // Remove the completed operation.
    // If this was the final pending operation, this instance should
    // no longer receive allocation and read callbacks.
    readOperations_.pop_front();
    if (readOperations_.empty()) {
      handle_->readStopFromLoop();
    }
  }
}

void Connection::Impl::writeCallbackFromLoop_(int status) {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(9) << "Connection " << id_ << " has completed a write request ("
             << formatUvError(status) << ")";

  if (status < 0) {
    setError_(TP_CREATE_ERROR(UVError, status));
    // Do NOT return, because the error handler method will only fire the
    // callbacks of the read operations, because we can only fire the callbacks
    // of the write operations after their corresponding UV requests complete
    // (or else the user may deallocate the buffers while the loop is still
    // processing them), therefore we must fire the write operation callbacks in
    // this method, both in case of success and of error.
  }

  TP_THROW_ASSERT_IF(writeOperations_.empty());
  auto& writeOperation = writeOperations_.front();
  writeOperation.callbackFromLoop(error_);
  writeOperations_.pop_front();
}

void Connection::Impl::closeCallbackFromLoop_() {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(9) << "Connection " << id_ << " has finished closing its handle";
  TP_DCHECK(writeOperations_.empty());
  leak_.reset();
}

void Connection::Impl::setError_(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError_();
}

void Connection::Impl::handleError_() {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(8) << "Connection " << id_ << " is handling error " << error_.what();

  for (auto& readOperation : readOperations_) {
    readOperation.callbackFromLoop(error_);
  }
  readOperations_.clear();
  // Do NOT fire the callbacks of the write operations, because we must wait for
  // their corresponding UV write requests to complete (or else the user may
  // deallocate the buffers while the loop is still processing them).
  handle_->closeFromLoop();
}

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<TCPHandle> handle,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(handle),
          std::move(id))) {
  impl_->init();
}

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(addr),
          std::move(id))) {
  impl_->init();
}

void Connection::Impl::init() {
  context_->deferToLoop([impl{shared_from_this()}]() { impl->initFromLoop(); });
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

void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  impl_->read(ptr, length, std::move(fn));
}

void Connection::Impl::read(void* ptr, size_t length, read_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, ptr, length, fn{std::move(fn)}]() mutable {
        impl->readFromLoop(ptr, length, std::move(fn));
      });
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

void Connection::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Connection::close() {
  impl_->close();
}

Connection::~Connection() {
  close();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
