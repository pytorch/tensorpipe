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
/// Producer of data for RingBuffer.
///
/// Provides method to write into ringbuffer.
///
class Producer : public RingBufferWrapper {
 public:
  // Use base class constructor.
  using RingBufferWrapper::RingBufferWrapper;

  Producer(const Producer&) = delete;
  Producer(Producer&&) = delete;

  //
  // Transaction based API.
  //
  // Only one writer can have an active transaction at any time.
  // *InTx* operations that fail do not cancel transaction.
  //
  bool inTx() const noexcept {
    return this->inTx_;
  }

  [[nodiscard]] ssize_t startTx() {
    if (unlikely(inTx())) {
      return -EBUSY;
    }
    if (this->header_.in_write_tx.test_and_set(std::memory_order_acquire)) {
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
    this->header_.incHead(this->tx_size_);
    this->tx_size_ = 0;
    // <in_write_tx> flags that we are in a transaction,
    // so enforce no stores pass it.
    this->inTx_ = false;
    this->header_.in_write_tx.clear(std::memory_order_release);
    return 0;
  }

  [[nodiscard]] ssize_t cancelTx() noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    this->tx_size_ = 0;
    // <in_write_tx> flags that we are in a transaction,
    // so enforce no stores pass it.
    this->inTx_ = false;
    this->header_.in_write_tx.clear(std::memory_order_release);
    return 0;
  }

  // TODO: Pass size first.
  [[nodiscard]] ssize_t write(const void* d, size_t size) noexcept {
    {
      auto ret = startTx();
      if (0 > ret) {
        return ret;
      }
    }
    auto ret = this->writeInTx(size, static_cast<const uint8_t*>(d));
    if (0 > ret) {
      auto r = cancelTx();
      TP_DCHECK_EQ(r, 0);
      return ret;
    }
    ret = commitTx();
    TP_DCHECK_EQ(ret, 0);
    return static_cast<ssize_t>(size);
  }

  [[nodiscard]] ssize_t writeInTx(size_t size, const void* data) noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    return this->copyToRingBuffer_(static_cast<const uint8_t*>(data), size);
  }

  [[nodiscard]] ssize_t writeAtMostInTx(
      size_t size,
      const void* data) noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }

    const auto space = this->header_.freeSizeWeak() - this->tx_size_;
    if (space == 0) {
      return -ENOSPC;
    }
    return this->writeInTx(std::min(size, space), data);
  }

  template <class T>
  [[nodiscard]] ssize_t writeInTx(const T& d) noexcept {
    return writeInTx(sizeof(T), &d);
  }

  [[nodiscard]] std::pair<ssize_t, void*> reserveContiguousInTx(
      const size_t size) {
    if (unlikely(size == 0)) {
      TP_LOG_WARNING() << "Reserve of size zero is not supported. "
                       << "Is size set correctly?";
      return {-EINVAL, nullptr};
    }

    if (unlikely(size > this->header_.kDataPoolByteSize)) {
      TP_LOG_WARNING() << "Asked to reserve " << size << " bytes in a buffer"
                       << " of size " << this->header_.kDataPoolByteSize;
      return {-EINVAL, nullptr};
    }

    // Single writer, safe to read head.
    uint64_t head = this->header_.readHead();
    uint64_t tail = this->header_.readTail();

    uint64_t used = head - tail;
    if (unlikely(used > this->header_.kDataPoolByteSize)) {
      TP_LOG_ERROR()
          << "number of used bytes found to be larger than ring buffer size";
      return {-EPERM, nullptr};
    }

    uint64_t space = this->header_.kDataPoolByteSize - used - this->tx_size_;
    if (space < size) {
      return {-ENOSPC, nullptr};
    }

    uint64_t start = (head + this->tx_size_) & this->header_.kDataModMask;
    uint64_t end = (start + size) & this->header_.kDataModMask;

    // Check if the chunk is contiguous.
    if (unlikely(end < start)) {
      end = this->header_.kDataPoolByteSize;
    }

    this->tx_size_ += end - start;

    return {end - start, &this->data_[start]};
  }

 protected:
  bool inTx_{false};

  // Return 0 if succeded, otherwise return negative error code.
  // If successful, increases tx_size_ by size.
  [[nodiscard]] ssize_t copyToRingBuffer_(
      const uint8_t* d,
      const size_t size) noexcept {
    TP_THROW_IF_NULLPTR(d);

    if (unlikely(size == 0)) {
      TP_LOG_WARNING() << "Copies of size zero are not supported. "
                       << "Is size set correctly?";
      return -EINVAL;
    }

    if (unlikely(size > this->header_.kDataPoolByteSize)) {
      TP_LOG_WARNING() << "Asked to write " << size << " bytes in a buffer"
                       << " of size " << this->header_.kDataPoolByteSize;
      return -EINVAL;
    }

    // Single writer, safe to read head.
    uint64_t head = this->header_.readHead();
    uint64_t tail = this->header_.readTail();

    uint64_t used = head - tail;
    if (unlikely(used > this->header_.kDataPoolByteSize)) {
      TP_LOG_ERROR()
          << "number of used bytes found to be larger than ring buffer size";
      return -EPERM;
    }

    // Check that there is enough space.
    uint64_t space = this->header_.kDataPoolByteSize - used;
    if (unlikely(this->tx_size_ + size > space)) {
      return -ENOSPC;
    }

    uint64_t start = (head + this->tx_size_) & this->header_.kDataModMask;
    uint64_t end = (start + size) & this->header_.kDataModMask;

    if (likely(start < end)) {
      // d's content won't wrap around end of buffer.
      std::memcpy(&this->data_[start], d, size);
    } else {
      // d's content will wrap around end of buffer.
      size_t size0 = this->header_.kDataPoolByteSize - start;
      std::memcpy(&this->data_[start], d, size0);
      std::memcpy(&this->data_[0], static_cast<const uint8_t*>(d) + size0, end);
    }
    this->tx_size_ += size;
    return static_cast<ssize_t>(size);
  }
};

} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
