/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/connection_impl.h>

#include <array>
#include <deque>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/stream_read_write_ops.h>
#include <tensorpipe/transport/uv/context_impl.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

ConnectionImpl::ConnectionImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::unique_ptr<TCPHandle> handle)
    : ConnectionImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      handle_(std::move(handle)) {}

ConnectionImpl::ConnectionImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string addr)
    : ConnectionImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      handle_(context_->createHandle()),
      sockaddr_(Sockaddr::createInetSockAddr(addr)) {}

void ConnectionImpl::initImplFromLoop() {
  context_->enroll(*this);

  TP_VLOG(9) << "Connection " << id_ << " is initializing in loop";

  if (sockaddr_.has_value()) {
    TP_THROW_ASSERT_IF(context_->closed());
    handle_->initFromLoop();
    handle_->connectFromLoop(sockaddr_.value(), [this](int status) {
      if (status < 0) {
        setError(TP_CREATE_ERROR(UVError, status));
      }
    });
  }
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop(); });
  handle_->armAllocCallbackFromLoop(
      [this](uv_buf_t* buf) { this->allocCallbackFromLoop(buf); });
  handle_->armReadCallbackFromLoop([this](ssize_t nread, const uv_buf_t* buf) {
    this->readCallbackFromLoop(nread, buf);
  });
}

void ConnectionImpl::readImplFromLoop(read_callback_fn fn) {
  readOperations_.emplace_back(std::move(fn));

  // Start reading if this is the first read operation.
  if (readOperations_.size() == 1) {
    handle_->readStartFromLoop();
  }
}

void ConnectionImpl::readImplFromLoop(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  readOperations_.emplace_back(ptr, length, std::move(fn));

  // Start reading if this is the first read operation.
  if (readOperations_.size() == 1) {
    handle_->readStartFromLoop();
  }
}

void ConnectionImpl::writeImplFromLoop(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  writeOperations_.emplace_back(ptr, length, std::move(fn));

  auto& writeOperation = writeOperations_.back();
  StreamWriteOperation::Buf* bufsPtr;
  unsigned int bufsLen;
  std::tie(bufsPtr, bufsLen) = writeOperation.getBufs();
  const std::array<uv_buf_t, 2> uvBufs = {
      uv_buf_t{bufsPtr[0].base, bufsPtr[0].len},
      uv_buf_t{bufsPtr[1].base, bufsPtr[1].len}};
  handle_->writeFromLoop(uvBufs.data(), bufsLen, [this](int status) {
    this->writeCallbackFromLoop(status);
  });
}

void ConnectionImpl::allocCallbackFromLoop(uv_buf_t* buf) {
  TP_DCHECK(context_->inLoop());
  TP_THROW_ASSERT_IF(readOperations_.empty());
  TP_VLOG(9) << "Connection " << id_
             << " has incoming data for which it needs to provide a buffer";
  readOperations_.front().allocFromLoop(&buf->base, &buf->len);
}

void ConnectionImpl::readCallbackFromLoop(
    ssize_t nread,
    const uv_buf_t* /* unused */) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_ << " has completed reading some data ("
             << (nread >= 0 ? std::to_string(nread) + " bytes"
                            : formatUvError(nread))
             << ")";

  if (nread < 0) {
    setError(TP_CREATE_ERROR(UVError, nread));
    return;
  }

  TP_THROW_ASSERT_IF(readOperations_.empty());
  auto& readOperation = readOperations_.front();
  readOperation.readFromLoop(nread);
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

void ConnectionImpl::writeCallbackFromLoop(int status) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_ << " has completed a write request ("
             << formatUvError(status) << ")";

  if (status < 0) {
    setError(TP_CREATE_ERROR(UVError, status));
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

void ConnectionImpl::closeCallbackFromLoop() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_ << " has finished closing its handle";
  TP_DCHECK(writeOperations_.empty());
  context_->unenroll(*this);
}

void ConnectionImpl::handleErrorImpl() {
  for (auto& readOperation : readOperations_) {
    readOperation.callbackFromLoop(error_);
  }
  readOperations_.clear();
  // Do NOT fire the callbacks of the write operations, because we must wait for
  // their corresponding UV write requests to complete (or else the user may
  // deallocate the buffers while the loop is still processing them).
  handle_->closeFromLoop();
  // Do NOT unenroll here, as we must keep the UV handle alive until the close
  // callback fires.
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
