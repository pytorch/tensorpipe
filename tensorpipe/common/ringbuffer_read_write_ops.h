/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <functional>
#include <memory>
#include <tuple>
#include <utility>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/nop.h>
#include <tensorpipe/common/ringbuffer_role.h>

namespace tensorpipe {

// Reads happen only if the user supplied a callback (and optionally
// a destination buffer). The callback is run from the event loop
// thread upon receiving a notification from our peer.
//
// The memory pointer argument to the callback is valid only for the
// duration of the callback. If the memory contents must be
// preserved for longer, it must be copied elsewhere.
//
class RingbufferReadOperation {
  enum Mode {
    READ_LENGTH,
    READ_PAYLOAD,
  };

 public:
  using read_callback_fn =
      std::function<void(const Error& error, const void* ptr, size_t len)>;
  // Read into a user-provided buffer of known length.
  inline RingbufferReadOperation(void* ptr, size_t len, read_callback_fn fn);
  // Read into an auto-allocated buffer, whose length is read from the wire.
  explicit inline RingbufferReadOperation(read_callback_fn fn);
  // Read into a user-provided libnop object, read length from the wire.
  inline RingbufferReadOperation(
      AbstractNopHolder* nopObject,
      read_callback_fn fn);

  // Processes a pending read.
  template <int NumRoles, int RoleIdx>
  inline size_t handleRead(RingBufferRole<NumRoles, RoleIdx>& inbox);

  bool completed() const {
    return (mode_ == READ_PAYLOAD && bytesRead_ == len_);
  }

  inline void handleError(const Error& error);

 private:
  Mode mode_{READ_LENGTH};
  void* ptr_{nullptr};
  AbstractNopHolder* nopObject_{nullptr};
  std::unique_ptr<uint8_t[]> buf_;
  size_t len_{0};
  size_t bytesRead_{0};
  read_callback_fn fn_;
  // Use a separare flag, rather than checking if ptr_ == nullptr, to catch the
  // case of a user explicitly passing in a nullptr with length zero, in which
  // case we must check that the length matches the header we see on the wire.
  const bool ptrProvided_;

  template <int NumRoles, int RoleIdx>
  inline ssize_t readNopObject(RingBufferRole<NumRoles, RoleIdx>& inbox);
};

// Writes happen only if the user supplied a memory pointer, the
// number of bytes to write, and a callback to execute upon
// completion of the write.
//
// The memory pointed to by the pointer may only be reused or freed
// after the callback has been called.
//
class RingbufferWriteOperation {
  enum Mode {
    WRITE_LENGTH,
    WRITE_PAYLOAD,
  };

 public:
  using write_callback_fn = std::function<void(const Error& error)>;
  // Write from a user-provided buffer of known length.
  inline RingbufferWriteOperation(
      const void* ptr,
      size_t len,
      write_callback_fn fn);
  // Write from a user-provided libnop object.
  inline RingbufferWriteOperation(
      const AbstractNopHolder* nopObject,
      write_callback_fn fn);

  template <int NumRoles, int RoleIdx>
  inline size_t handleWrite(RingBufferRole<NumRoles, RoleIdx>& outbox);

  bool completed() const {
    return (mode_ == WRITE_PAYLOAD && bytesWritten_ == len_);
  }

  inline void handleError(const Error& error);

 private:
  Mode mode_{WRITE_LENGTH};
  const void* ptr_{nullptr};
  const AbstractNopHolder* nopObject_{nullptr};
  size_t len_{0};
  size_t bytesWritten_{0};
  write_callback_fn fn_;

  template <int NumRoles, int RoleIdx>
  inline ssize_t writeNopObject(RingBufferRole<NumRoles, RoleIdx>& outbox);
};

RingbufferReadOperation::RingbufferReadOperation(
    void* ptr,
    size_t len,
    read_callback_fn fn)
    : ptr_(ptr), len_(len), fn_(std::move(fn)), ptrProvided_(true) {}

RingbufferReadOperation::RingbufferReadOperation(read_callback_fn fn)
    : fn_(std::move(fn)), ptrProvided_(false) {}

RingbufferReadOperation::RingbufferReadOperation(
    AbstractNopHolder* nopObject,
    read_callback_fn fn)
    : nopObject_(nopObject), fn_(std::move(fn)), ptrProvided_(false) {}

template <int NumRoles, int RoleIdx>
size_t RingbufferReadOperation::handleRead(
    RingBufferRole<NumRoles, RoleIdx>& inbox) {
  ssize_t ret;
  size_t bytesReadNow = 0;

  // Start read transaction. This end of the connection is the only consumer for
  // this ringbuffer, and all reads are done from the reactor thread, so there
  // cannot be another transaction already going on. Fail hard in case.
  ret = inbox.startTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  if (mode_ == READ_LENGTH) {
    uint32_t length;
    ret = inbox.template readInTx</*AllowPartial=*/false>(
        &length, sizeof(length));
    if (likely(ret >= 0)) {
      mode_ = READ_PAYLOAD;
      bytesReadNow += ret;
      if (nopObject_ != nullptr) {
        len_ = length;
      } else if (ptrProvided_) {
        TP_DCHECK_EQ(length, len_);
      } else {
        len_ = length;
        buf_ = std::make_unique<uint8_t[]>(len_);
        ptr_ = buf_.get();
      }
    } else if (unlikely(ret != -ENODATA)) {
      TP_THROW_SYSTEM(-ret);
    }
  }

  if (mode_ == READ_PAYLOAD) {
    if (nopObject_ != nullptr) {
      ret = readNopObject(inbox);
    } else {
      ret = inbox.template readInTx</*AllowPartial=*/true>(
          reinterpret_cast<uint8_t*>(ptr_) + bytesRead_, len_ - bytesRead_);
    }
    if (likely(ret >= 0)) {
      bytesRead_ += ret;
      bytesReadNow += ret;
    } else if (unlikely(ret != -ENODATA)) {
      TP_THROW_SYSTEM(-ret);
    }
  }

  ret = inbox.commitTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  if (completed()) {
    fn_(Error::kSuccess, ptr_, len_);
  }

  return bytesReadNow;
}

template <int NumRoles, int RoleIdx>
ssize_t RingbufferReadOperation::readNopObject(
    RingBufferRole<NumRoles, RoleIdx>& inbox) {
  TP_THROW_ASSERT_IF(len_ > inbox.getSize());

  ssize_t numBuffers;
  std::array<typename RingBufferRole<NumRoles, RoleIdx>::Buffer, 2> buffers;
  std::tie(numBuffers, buffers) =
      inbox.template accessContiguousInTx</*AllowPartial=*/false>(len_);
  if (unlikely(numBuffers < 0)) {
    return numBuffers;
  }

  NopReader reader(
      buffers[0].ptr, buffers[0].len, buffers[1].ptr, buffers[1].len);
  nop::Status<void> status = nopObject_->read(reader);
  if (status.error() == nop::ErrorStatus::ReadLimitReached) {
    return -ENODATA;
  } else if (status.has_error()) {
    return -EINVAL;
  }

  return len_;
}

void RingbufferReadOperation::handleError(const Error& error) {
  fn_(error, nullptr, 0);
}

RingbufferWriteOperation::RingbufferWriteOperation(
    const void* ptr,
    size_t len,
    write_callback_fn fn)
    : ptr_(ptr), len_(len), fn_(std::move(fn)) {}

RingbufferWriteOperation::RingbufferWriteOperation(
    const AbstractNopHolder* nopObject,
    write_callback_fn fn)
    : nopObject_(nopObject), len_(nopObject_->getSize()), fn_(std::move(fn)) {}

template <int NumRoles, int RoleIdx>
size_t RingbufferWriteOperation::handleWrite(
    RingBufferRole<NumRoles, RoleIdx>& outbox) {
  ssize_t ret;
  size_t bytesWrittenNow = 0;

  // Start write transaction. This end of the connection is the only producer
  // for this ringbuffer, and all writes are done from the reactor thread, so
  // there cannot be another transaction already going on. Fail hard in case.
  ret = outbox.startTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  if (mode_ == WRITE_LENGTH) {
    uint32_t length = len_;
    ret = outbox.template writeInTx</*AllowPartial=*/false>(
        &length, sizeof(length));
    if (likely(ret >= 0)) {
      mode_ = WRITE_PAYLOAD;
      bytesWrittenNow += ret;
    } else if (unlikely(ret != -ENODATA)) {
      TP_THROW_SYSTEM(-ret);
    }
  }

  if (mode_ == WRITE_PAYLOAD) {
    if (nopObject_ != nullptr) {
      ret = writeNopObject(outbox);
    } else {
      ret = outbox.template writeInTx</*AllowPartial=*/true>(
          reinterpret_cast<const uint8_t*>(ptr_) + bytesWritten_,
          len_ - bytesWritten_);
    }
    if (likely(ret >= 0)) {
      bytesWritten_ += ret;
      bytesWrittenNow += ret;
    } else if (unlikely(ret != -ENODATA)) {
      TP_THROW_SYSTEM(-ret);
    }
  }

  ret = outbox.commitTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  if (completed()) {
    fn_(Error::kSuccess);
  }

  return bytesWrittenNow;
}

template <int NumRoles, int RoleIdx>
ssize_t RingbufferWriteOperation::writeNopObject(
    RingBufferRole<NumRoles, RoleIdx>& outbox) {
  TP_THROW_ASSERT_IF(len_ > outbox.getSize());

  ssize_t numBuffers;
  std::array<typename RingBufferRole<NumRoles, RoleIdx>::Buffer, 2> buffers;
  std::tie(numBuffers, buffers) =
      outbox.template accessContiguousInTx</*AllowPartial=*/false>(len_);
  if (unlikely(numBuffers < 0)) {
    return numBuffers;
  }

  NopWriter writer(
      buffers[0].ptr, buffers[0].len, buffers[1].ptr, buffers[1].len);
  nop::Status<void> status = nopObject_->write(writer);
  if (status.error() == nop::ErrorStatus::WriteLimitReached) {
    return -ENODATA;
  } else if (status.has_error()) {
    return -EINVAL;
  }

  return len_;
}

void RingbufferWriteOperation::handleError(const Error& error) {
  fn_(error);
}

} // namespace tensorpipe
