/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/util/ringbuffer/ringbuffer.h>

namespace tensorpipe {
namespace util {
namespace ringbuffer {

///
/// Consumer of data for a RingBuffer.
///
/// Provides method to read data ringbuffer.
///
class Consumer : public RingBufferWrapper {
 public:
  // Use base class constructor.
  using RingBufferWrapper::RingBufferWrapper;

  Consumer(const Consumer&) = delete;
  Consumer(Consumer&&) = delete;

  virtual ~Consumer() {
    if (inTx()) {
      auto r = this->cancelTx();
      TP_DCHECK_EQ(r, 0);
    }
  }

  //
  // Transaction based API.
  // Only one writer can have an active transaction at any time.
  // *InTx* operations that fail do not cancel transaction.
  //
  bool inTx() const noexcept {
    return this->inTx_;
  }

  [[nodiscard]] ssize_t startTx() noexcept {
    if (unlikely(inTx())) {
      return -EBUSY;
    }
    if (this->header_.in_read_tx.test_and_set(std::memory_order_acquire)) {
      return -EAGAIN;
    }
    this->inTx_ = true;
    TP_DCHECK_EQ(this->tx_size_, 0);
    return 0;
  }

  [[nodiscard]] ssize_t commitTx() noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    this->header_.incTail(this->tx_size_);
    this->tx_size_ = 0;
    this->inTx_ = false;
    this->header_.in_read_tx.clear(std::memory_order_release);
    return 0;
  }

  [[nodiscard]] ssize_t cancelTx() noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    this->tx_size_ = 0;
    // <in_read_tx> flags that we are in a transaction,
    // so enforce no stores pass it.
    this->inTx_ = false;
    this->header_.in_read_tx.clear(std::memory_order_release);
    return 0;
  }

  // Copy next <size> bytes to buffer.
  [[nodiscard]] ssize_t copyInTx(const size_t size, void* buffer) noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    if (size == 0) {
      return 0;
    }
    if (unlikely(size > this->header_.kDataPoolByteSize)) {
      return -EINVAL;
    }
    const uint8_t* ptr =
        readFromRingBuffer_(size, static_cast<uint8_t*>(buffer));
    if (ptr == nullptr) {
      return -ENODATA;
    }
    return static_cast<ssize_t>(size);
  }

  // Copy up to the next <size> bytes to buffer.
  [[nodiscard]] ssize_t copyAtMostInTx(
      const size_t size,
      uint8_t* buffer) noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    // Single reader because we are in Tx, safe to read tail.
    const size_t avail = this->header_.usedSizeWeak() - this->tx_size_;
    return this->copyInTx(std::min(size, avail), buffer);
  }

  // Return the number of bytes (up to size) available as a contiguous segment,
  // as well as a pointer to the first byte.
  [[nodiscard]] std::pair<ssize_t, const uint8_t*> readContiguousAtMostInTx(
      const size_t size) noexcept {
    if (unlikely(!inTx())) {
      return {-EINVAL, nullptr};
    }

    const uint64_t head = this->header_.readHead();
    // Single reader because we are in Tx, safe to read tail.
    const uint64_t tail = this->header_.readTail();
    const uint64_t start = (this->tx_size_ + tail) & this->header_.kDataModMask;
    const size_t avail = head - tail - this->tx_size_;
    const uint64_t end = std::min(
        start + std::min(size, avail), this->header_.kDataPoolByteSize);

    this->tx_size_ += end - start;

    return {end - start, &this->data_[start]};
  }

  //
  // High-level atomic operations.
  //

  /// Makes a copy to <buffer>, buffer must be of size <size> or larger.
  [[nodiscard]] ssize_t copy(const size_t size, void* buffer) noexcept {
    auto ret = startTx();
    if (0 > ret) {
      return ret;
    }

    ret = copyInTx(size, static_cast<uint8_t*>(buffer));
    if (0 > ret) {
      auto r = cancelTx();
      TP_DCHECK_EQ(r, 0);
      return ret;
    }
    TP_DCHECK_EQ(ret, size);

    ret = commitTx();
    TP_DCHECK_EQ(ret, 0);
    return static_cast<ssize_t>(size);
  }

 protected:
  bool inTx_{false};

  /// Returns a ptr to data (or null if no data available).
  /// If succeeds, increases <tx_size_> by size.
  const uint8_t* readFromRingBuffer_(
      const size_t size,
      uint8_t* copy_buffer) noexcept {
    // Caller must have taken care of this.
    TP_DCHECK_LE(size, this->header_.kDataPoolByteSize);

    if (unlikely(0 >= size)) {
      TP_LOG_ERROR() << "Cannot copy value of zero size";
      return nullptr;
    }

    if (unlikely(size > this->header_.kDataPoolByteSize)) {
      TP_LOG_ERROR() << "reads larger than buffer are not supported";
      return nullptr;
    }

    const uint64_t head = this->header_.readHead();
    // Single reader because we are in Tx, safe to read tail.
    const uint64_t tail = this->header_.readTail();

    TP_DCHECK_LE(head - tail, this->header_.kDataPoolByteSize);

    // Check if there is enough data.
    if (this->tx_size_ + size > head - tail) {
      return nullptr;
    }

    // start and end are head and tail in module arithmetic.
    const uint64_t start = (this->tx_size_ + tail) & this->header_.kDataModMask;
    const uint64_t end = (start + size) & this->header_.kDataModMask;

    const bool wrap = start >= end;

    this->tx_size_ += size;

    if (likely(!wrap)) {
      std::memcpy(copy_buffer, &this->data_[start], size);
    } else {
      const size_t size0 = this->header_.kDataPoolByteSize - start;
      std::memcpy(copy_buffer, &this->data_[start], size0);
      std::memcpy(copy_buffer + size0, &this->data_[0], end);
    }

    return copy_buffer;
  }
};

} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
