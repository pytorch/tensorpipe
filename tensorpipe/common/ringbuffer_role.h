/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstring>
#include <tuple>
#include <utility>

#include <tensorpipe/common/ringbuffer.h>

namespace tensorpipe {

///
/// Role of a RingBuffer.
///
/// Provides methods to read and write data into a ringbuffer.
///
template <int NumRoles, int RoleIdx>
class RingBufferRole {
 public:
  static_assert(0 <= RoleIdx && RoleIdx < NumRoles, "");

  RingBufferRole() = delete;

  explicit RingBufferRole(RingBuffer<NumRoles>& rb)
      : header_{rb.getHeader()}, data_{rb.getData()} {
    TP_THROW_IF_NULLPTR(data_);
  }

  RingBufferRole(const RingBufferRole&) = delete;
  RingBufferRole(RingBufferRole&&) = delete;

  RingBufferRole& operator=(const RingBufferRole&) = delete;
  RingBufferRole& operator=(RingBufferRole&&) = delete;

  ~RingBufferRole() noexcept {
    TP_THROW_ASSERT_IF(inTx());
  }

  size_t getSize() const {
    return header_.kDataPoolByteSize;
  }

  //
  // Transaction based API.
  //
  // Only one instance of a role can have an active transaction at any time.
  // *InTx* operations that fail do not cancel transaction.
  //
  bool inTx() const noexcept {
    return inTx_;
  }

  [[nodiscard]] ssize_t startTx() noexcept {
    if (unlikely(inTx())) {
      return -EBUSY;
    }
    if (header_.template beginTransaction<RoleIdx>()) {
      return -EAGAIN;
    }
    inTx_ = true;
    TP_DCHECK_EQ(txSize_, 0);
    return 0;
  }

  [[nodiscard]] ssize_t commitTx() noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    header_.template incMarker<RoleIdx>(txSize_);
    txSize_ = 0;
    inTx_ = false;
    header_.template endTransaction<RoleIdx>();
    return 0;
  }

  [[nodiscard]] ssize_t cancelTx() noexcept {
    if (unlikely(!inTx())) {
      return -EINVAL;
    }
    txSize_ = 0;
    inTx_ = false;
    header_.template endTransaction<RoleIdx>();
    return 0;
  }

  struct Buffer {
    uint8_t* ptr{nullptr};
    size_t len{0};
  };

  // The first item is negative in case of error, otherwise it contains how many
  // elements of the array are valid (0, 1 or 2). The elements are ptr+len pairs
  // of contiguous areas of the ringbuffer that, chained together, represent a
  // slice of the requested size (or less if not enough data is available, and
  // AllowPartial is set to true).
  template <bool AllowPartial>
  [[nodiscard]] std::pair<ssize_t, std::array<Buffer, 2>> accessContiguousInTx(
      size_t size) noexcept {
    std::array<Buffer, 2> result;

    if (unlikely(!inTx())) {
      return {-EINVAL, result};
    }

    if (unlikely(size == 0)) {
      return {0, result};
    }

    const uint64_t tail = header_.template readMarker<RoleIdx>();
    const uint64_t head =
        header_.template readMarker<(RoleIdx + 1) % NumRoles>() +
        (RoleIdx + 1 == NumRoles ? header_.kDataPoolByteSize : 0);
    TP_DCHECK_LE(head - tail, header_.kDataPoolByteSize);

    const size_t avail = head - tail - txSize_;
    TP_DCHECK_GE(avail, 0);

    if (!AllowPartial && avail < size) {
      return {-ENODATA, result};
    }

    if (avail == 0) {
      return {0, result};
    }

    size = std::min(size, avail);

    const uint64_t start = (tail + txSize_) & header_.kDataModMask;
    const uint64_t end = (start + size) & header_.kDataModMask;

    txSize_ += size;

    // end == 0 is the same as end == bufferSize, in which case it doesn't wrap.
    const bool wrap = (start >= end && end > 0);
    if (likely(!wrap)) {
      result[0] = {.ptr = data_ + start, .len = size};
      return {1, result};
    } else {
      result[0] = {
          .ptr = data_ + start, .len = header_.kDataPoolByteSize - start};
      result[1] = {.ptr = data_, .len = end};
      return {2, result};
    }
  }

  // Increment our marker without doing anything, i.e., "skip" over the data.
  [[nodiscard]] ssize_t incMarkerInTx(size_t size) {
    // We could implement this from scratch but we'd rather re-use the logic
    // from accessContiguous as it's easy to get it wrong.
    ssize_t ret;
    std::array<Buffer, 2> buffers;
    std::tie(ret, buffers) = accessContiguousInTx</*allowPartial=*/false>(size);
    return ret;
  }

  // Copy data from the ringbuffer into the provided buffer, up to the given
  // size (only copy less data if AllowPartial is set to true).
  template <bool AllowPartial>
  [[nodiscard]] ssize_t readInTx(void* buffer, const size_t size) noexcept {
    ssize_t numBuffers;
    std::array<Buffer, 2> buffers;
    std::tie(numBuffers, buffers) = accessContiguousInTx<AllowPartial>(size);

    if (unlikely(numBuffers < 0)) {
      return numBuffers;
    }

    if (unlikely(numBuffers == 0)) {
      // Nothing to do.
      return 0;
    } else if (likely(numBuffers == 1)) {
      std::memcpy(buffer, buffers[0].ptr, buffers[0].len);
      return buffers[0].len;
    } else if (likely(numBuffers == 2)) {
      std::memcpy(buffer, buffers[0].ptr, buffers[0].len);
      std::memcpy(
          reinterpret_cast<uint8_t*>(buffer) + buffers[0].len,
          buffers[1].ptr,
          buffers[1].len);
      return buffers[0].len + buffers[1].len;
    } else {
      TP_THROW_ASSERT() << "Bad number of buffers: " << numBuffers;
      // Dummy return to make the compiler happy.
      return -EINVAL;
    }
  }

  // Copy data from the provided buffer into the ringbuffer, up to the given
  // size (only copy less data if AllowPartial is set to true).
  template <bool AllowPartial>
  [[nodiscard]] ssize_t writeInTx(
      const void* buffer,
      const size_t size) noexcept {
    ssize_t numBuffers;
    std::array<Buffer, 2> buffers;
    std::tie(numBuffers, buffers) = accessContiguousInTx<AllowPartial>(size);

    if (unlikely(numBuffers < 0)) {
      return numBuffers;
    }

    if (unlikely(numBuffers == 0)) {
      // Nothing to do.
      return 0;
    } else if (likely(numBuffers == 1)) {
      std::memcpy(buffers[0].ptr, buffer, buffers[0].len);
      return buffers[0].len;
    } else if (likely(numBuffers == 2)) {
      std::memcpy(buffers[0].ptr, buffer, buffers[0].len);
      std::memcpy(
          buffers[1].ptr,
          reinterpret_cast<const uint8_t*>(buffer) + buffers[0].len,
          buffers[1].len);
      return buffers[0].len + buffers[1].len;
    } else {
      TP_THROW_ASSERT() << "Bad number of buffers: " << numBuffers;
      // Dummy return to make the compiler happy.
      return -EINVAL;
    }
  }

  //
  // High-level atomic operations.
  //

  // Copy data from the ringbuffer into the provided buffer, exactly the given
  // size. Take care of opening and closing the transaction.
  [[nodiscard]] ssize_t read(void* buffer, const size_t size) noexcept {
    auto ret = startTx();
    if (0 > ret) {
      return ret;
    }

    ret = readInTx</*AllowPartial=*/false>(buffer, size);
    if (0 > ret) {
      auto r = cancelTx();
      TP_DCHECK_EQ(r, 0);
      return ret;
    }
    TP_DCHECK_EQ(ret, size);

    ret = commitTx();
    TP_DCHECK_EQ(ret, 0);

    return size;
  }

  // Copy data from the provided buffer into the ringbuffer, exactly the given
  // size. Take care of opening and closing the transaction.
  [[nodiscard]] ssize_t write(const void* buffer, size_t size) noexcept {
    auto ret = startTx();
    if (0 > ret) {
      return ret;
    }

    ret = writeInTx</*AllowPartial=*/false>(buffer, size);
    if (0 > ret) {
      auto r = cancelTx();
      TP_DCHECK_EQ(r, 0);
      return ret;
    }
    TP_DCHECK_EQ(ret, size);

    ret = commitTx();
    TP_DCHECK_EQ(ret, 0);

    return size;
  }

 private:
  RingBufferHeader<NumRoles>& header_;
  uint8_t* const data_;
  unsigned txSize_ = 0;
  bool inTx_{false};
};

} // namespace tensorpipe
