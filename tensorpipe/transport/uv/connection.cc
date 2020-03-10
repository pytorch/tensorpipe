/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/connection.h>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::shared_ptr<Connection> Connection::create(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  auto handle = TCPHandle::create(loop);
  loop->deferToLoop([handle, addr]() {
    handle->initFromLoop();
    handle->connectFromLoop(addr);
  });
  auto conn = std::make_shared<Connection>(
      ConstructorToken(), std::move(loop), std::move(handle));
  conn->init();
  return conn;
}

std::shared_ptr<Connection> Connection::create(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle) {
  auto conn = std::make_shared<Connection>(
      ConstructorToken(), std::move(loop), std::move(handle));
  conn->init();
  return conn;
}

Connection::Connection(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(loop), impl_(std::make_shared<Impl>(loop, std::move(handle))) {}

Connection::Impl::Impl(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(std::move(loop)), handle_(std::move(handle)) {}

Connection::~Connection() {
  loop_->deferToLoop([impl{impl_}]() { impl->closeFromLoop(); });
}

void Connection::Impl::closeFromLoop() {
  TP_DCHECK(loop_->inLoopThread());
  handle_->closeFromLoop();
}

void Connection::Impl::closeCallbackFromLoop() {
  TP_DCHECK(loop_->inLoopThread());
  leak_.reset();
}

void Connection::init() {
  loop_->deferToLoop([impl{impl_}]() { impl->initFromLoop(); });
}

void Connection::Impl::initFromLoop() {
  leak_ = shared_from_this();
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop(); });
  handle_->armAllocCallbackFromLoop(
      [this](uv_buf_t* buf) { this->allocCallbackFromLoop(buf); });
  handle_->armReadCallbackFromLoop([this](ssize_t nread, const uv_buf_t* buf) {
    this->readCallbackFromLoop(nread, buf);
  });
}

void Connection::read(read_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, fn{std::move(fn)}]() mutable {
    impl->readFromLoop(std::move(fn));
  });
}

void Connection::Impl::readFromLoop(read_callback_fn fn) {
  TP_DCHECK(loop_->inLoopThread());
  std::unique_lock<std::mutex> lock(readOperationsMutex_);
  readOperations_.emplace_back(std::move(fn));
  // Start reading if this is the first read operation.
  if (readOperations_.size() == 1) {
    handle_->readStartFromLoop();
  }
}

void Connection::read(void* ptr, size_t length, read_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, ptr, length, fn{std::move(fn)}]() mutable {
    impl->readFromLoop(ptr, length, std::move(fn));
  });
}

void Connection::Impl::readFromLoop(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  TP_DCHECK(loop_->inLoopThread());
  std::unique_lock<std::mutex> lock(readOperationsMutex_);
  readOperations_.emplace_back(ptr, length, std::move(fn));
  // Start reading if this is the first read operation.
  if (readOperations_.size() == 1) {
    handle_->readStartFromLoop();
  }
}

void Connection::write(const void* ptr, size_t length, write_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, ptr, length, fn{std::move(fn)}]() mutable {
    impl->writeFromLoop(ptr, length, std::move(fn));
  });
}

void Connection::Impl::writeFromLoop(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  TP_DCHECK(loop_->inLoopThread());
  std::unique_lock<std::mutex> lock(writeOperationsMutex_);
  writeOperations_.emplace_back(ptr, length, std::move(fn));
  auto& writeOperation = writeOperations_.back();

  // Populate uv_buf_t array that we'll write for this operation.
  auto bufs = std::shared_ptr<uv_buf_t>(
      new uv_buf_t[2], std::default_delete<uv_buf_t[]>());
  uv_buf_t* bufs_ptr = bufs.get();
  unsigned int bufs_len = 2;
  bufs_ptr[0].base =
      const_cast<char*>(reinterpret_cast<const char*>(&writeOperation.length));
  bufs_ptr[0].len = sizeof(writeOperation.length);
  bufs_ptr[1].base = const_cast<char*>(writeOperation.ptr);
  bufs_ptr[1].len = writeOperation.length;

  handle_->writeFromLoop(
      bufs_ptr, bufs_len, [this, bufs{std::move(bufs)}](int status) {
        this->writeCallbackFromLoop(status);
      });
}

void Connection::Impl::ReadOperation::allocFromLoop(uv_buf_t* buf) {
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

void Connection::Impl::ReadOperation::readFromLoop(
    ssize_t nread,
    const uv_buf_t* buf) {
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

void Connection::Impl::allocCallbackFromLoop(uv_buf_t* buf) {
  TP_DCHECK(loop_->inLoopThread());
  std::unique_lock<std::mutex> lock(readOperationsMutex_);
  TP_THROW_ASSERT_IF(readOperations_.empty());
  readOperations_.front().allocFromLoop(buf);
}

void Connection::Impl::readCallbackFromLoop(
    ssize_t nread,
    const uv_buf_t* buf) {
  TP_DCHECK(loop_->inLoopThread());
  std::unique_lock<std::mutex> lock(readOperationsMutex_);
  if (nread < 0) {
    error_ = TP_CREATE_ERROR(UVError, nread);
    while (!readOperations_.empty()) {
      auto& readOperation = readOperations_.front();
      // Execute callback without holding the operations lock.
      // The callback could issue another read.
      lock.unlock();
      readOperation.callbackFromLoop(error_);
      lock.lock();
      // Remove the completed operation.
      // If this was the final pending operation, this instance should
      // no longer receive allocation and read callbacks.
      readOperations_.pop_front();
      if (readOperations_.empty()) {
        handle_->readStopFromLoop();
      }
    }
    return;
  }

  TP_THROW_ASSERT_IF(readOperations_.empty());
  auto& readOperation = readOperations_.front();
  readOperation.readFromLoop(nread, buf);
  if (readOperation.completeFromLoop()) {
    // Execute callback without holding the operations lock.
    // The callback could issue another read.
    lock.unlock();
    readOperation.callbackFromLoop(Error::kSuccess);
    lock.lock();
    // Remove the completed operation.
    // If this was the final pending operation, this instance should
    // no longer receive allocation and read callbacks.
    readOperations_.pop_front();
    if (readOperations_.empty()) {
      handle_->readStopFromLoop();
    }
  }
}

void Connection::Impl::writeCallbackFromLoop(int status) {
  TP_DCHECK(loop_->inLoopThread());
  std::unique_lock<std::mutex> lock(writeOperationsMutex_);
  TP_DCHECK(!writeOperations_.empty());

  // Move write operation to the stack.
  auto writeOperation = std::move(writeOperations_.front());
  writeOperations_.pop_front();

  // Execute callback without holding the operations lock.
  // The callback could issue another write.
  lock.unlock();
  if (status == 0) {
    writeOperation.callbackFromLoop(Error::kSuccess);
  } else {
    writeOperation.callbackFromLoop(TP_CREATE_ERROR(UVError, status));
  }
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
