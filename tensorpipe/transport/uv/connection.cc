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
      if (givenLength_) {
        TP_DCHECK(ptr_ != nullptr);
        TP_DCHECK_EQ(readLength_, givenLength_.value());
      } else {
        TP_DCHECK(ptr_ == nullptr);
        buffer_ = std::make_unique<char[]>(readLength_);
        ptr_ = buffer_.get();
      }
      mode_ = READ_PAYLOAD;
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
  return std::make_tuple(bufs_.data(), bufs_.size());
}

void WriteOperation::callbackFromLoop(const Error& error) {
  fn_(error);
}

} // namespace

class Connection::Impl : public std::enable_shared_from_this<Connection::Impl> {
 public:
  Impl(std::shared_ptr<Loop>, std::shared_ptr<TCPHandle>);
  Impl(std::shared_ptr<Loop>, const Sockaddr&);

  // Called to initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Called to queue a read operation.
  void readFromLoop(read_callback_fn fn);
  void readFromLoop(void* ptr, size_t length, read_callback_fn fn);

  // Called to perform a write operation.
  void writeFromLoop(const void* ptr, size_t length, write_callback_fn fn);

  // Called to shut down the connection and its resources.
  void close();
  void closeFromLoop();

 private:
  // Called when libuv is about to read data from connection.
  void allocCallbackFromLoop_(uv_buf_t* buf);

  // Called when libuv has read data from connection.
  void readCallbackFromLoop_(ssize_t nread, const uv_buf_t* buf);

  // Called when libuv has written data to connection.
  void writeCallbackFromLoop_(int status);

  // Called when libuv has closed the handle.
  void closeCallbackFromLoop_();

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<TCPHandle> handle_;
  Error error_{Error::kSuccess};
  ClosingReceiver closingReceiver_;

  std::deque<ReadOperation> readOperations_;
  std::deque<WriteOperation> writeOperations_;

  // By having the instance store a shared_ptr to itself we create a reference
  // cycle which will "leak" the instance. This allows us to detach its
  // lifetime from the connection and sync it with the TCPHandle's life cycle.
  std::shared_ptr<Impl> leak_;
};

Connection::Impl::Impl(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(std::move(loop)),
      handle_(std::move(handle)),
      closingReceiver_(loop_, loop_->closingEmitter_) {}

Connection::Impl::Impl(std::shared_ptr<Loop> loop, const Sockaddr& addr)
    : loop_(std::move(loop)),
      handle_(TCPHandle::create(loop_)),
      closingReceiver_(loop_, loop_->closingEmitter_) {
  loop_->deferToLoop([this, addr]() {
    handle_->initFromLoop();
    handle_->connectFromLoop(addr, [this](int status) {
      if (status < 0) {
        error_ = TP_CREATE_ERROR(UVError, status);
        closeFromLoop();
      }
    });
  });
}

void Connection::Impl::initFromLoop() {
  leak_ = shared_from_this();
  closingReceiver_.activate(*this);
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop_(); });
  handle_->armAllocCallbackFromLoop(
      [this](uv_buf_t* buf) { this->allocCallbackFromLoop_(buf); });
  handle_->armReadCallbackFromLoop([this](ssize_t nread, const uv_buf_t* buf) {
    this->readCallbackFromLoop_(nread, buf);
  });
}

void Connection::Impl::readFromLoop(read_callback_fn fn) {
  TP_DCHECK(loop_->inLoopThread());

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
  TP_DCHECK(loop_->inLoopThread());

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
  TP_DCHECK(loop_->inLoopThread());

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

void Connection::Impl::close() {
  loop_->deferToLoop([impl{shared_from_this()}]() { impl->closeFromLoop(); });
}

void Connection::Impl::closeFromLoop() {
  TP_DCHECK(loop_->inLoopThread());
  handle_->closeFromLoop();
}

void Connection::Impl::allocCallbackFromLoop_(uv_buf_t* buf) {
  TP_DCHECK(loop_->inLoopThread());
  TP_THROW_ASSERT_IF(readOperations_.empty());
  readOperations_.front().allocFromLoop(buf);
}

void Connection::Impl::readCallbackFromLoop_(
    ssize_t nread,
    const uv_buf_t* buf) {
  TP_DCHECK(loop_->inLoopThread());
  if (nread < 0) {
    if (!error_) {
      error_ = TP_CREATE_ERROR(UVError, nread);
      closeFromLoop();
    }
    return;
  }

  TP_THROW_ASSERT_IF(readOperations_.empty());
  auto& readOperation = readOperations_.front();
  readOperation.readFromLoop(nread, buf);
  if (readOperation.completeFromLoop()) {
    readOperation.callbackFromLoop(error_);
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
  TP_DCHECK(loop_->inLoopThread());
  TP_DCHECK(!writeOperations_.empty());

  if (status < 0 && !error_) {
    error_ = TP_CREATE_ERROR(UVError, status);
    closeFromLoop();
  }

  auto& writeOperation = writeOperations_.front();
  writeOperation.callbackFromLoop(error_);
  writeOperations_.pop_front();
}

void Connection::Impl::closeCallbackFromLoop_() {
  TP_DCHECK(loop_->inLoopThread());
  if (!error_) {
    error_ = TP_CREATE_ERROR(UVError, UV_ECANCELED);
  }
  while (!readOperations_.empty()) {
    auto& readOperation = readOperations_.front();
    readOperation.callbackFromLoop(error_);
    // Remove the completed operation.
    readOperations_.pop_front();
  }
  TP_DCHECK(writeOperations_.empty());
  leak_.reset();
}

std::shared_ptr<Connection> Connection::create_(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  return std::make_shared<Connection>(
      ConstructorToken(), std::move(loop), addr);
}

std::shared_ptr<Connection> Connection::create_(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle) {
  return std::make_shared<Connection>(
      ConstructorToken(), std::move(loop), std::move(handle));
}

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(loop), impl_(std::make_shared<Impl>(loop, std::move(handle))) {
  loop_->deferToLoop([impl{impl_}]() { impl->initFromLoop(); });
}

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr)
    : loop_(loop), impl_(std::make_shared<Impl>(loop, addr)) {
  loop_->deferToLoop([impl{impl_}]() { impl->initFromLoop(); });
}

void Connection::read(read_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, fn{std::move(fn)}]() mutable {
    impl->readFromLoop(std::move(fn));
  });
}

void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, ptr, length, fn{std::move(fn)}]() mutable {
    impl->readFromLoop(ptr, length, std::move(fn));
  });
}

void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, ptr, length, fn{std::move(fn)}]() mutable {
    impl->writeFromLoop(ptr, length, std::move(fn));
  });
}

void Connection::close() {
  loop_->deferToLoop([impl{impl_}]() { impl->closeFromLoop(); });
}

Connection::~Connection() {
  close();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
