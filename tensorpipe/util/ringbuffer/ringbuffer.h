/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/types.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <type_traits>

#include <tensorpipe/common/system.h>

///
/// C++17 implementation of shared-memory friendly perf_event style ringbuffer.
/// It's designed to avoid parallel access and provide (almost) zero-copy
///
///
/// A ringbuffer has a header and a data members that can be allocated
/// independently from the ringbuffer object, allowing the ringbuffer object
/// to be stored in process' exclusive memory while header and data
/// could be in shared memory.
///
/// Multiple ringbuffers can reference the same header + data.
///
/// Multiple producers (or consumers) can reference the same ringbuffer.
///
/// Synchronization between all producers/consumers of all ringbuffers that
/// reference the same header + pair pairs is done using atomic operations
/// care is taken to guarantee lock-free implementations, reduce the usage
/// of LOCK prefixes and the access to non-exclusive cache lines by CPUs.
///
/// Producers write data atomically at ringbuffer's head, while Consumers
/// write data atomically at ringbuffer's tail.
///

namespace tensorpipe {
namespace util {
namespace ringbuffer {

///
/// RingBufferHeader contains the head, tail and other control information
/// of the RingBuffer.
///
/// <kMinByteSize_> is the minimum byte size of the circular buffer. The actual
/// size is the smallest power of 2 larger than kMinByteSize_. Enforcing the
/// size to be a power of two avoids costly division/modulo operations.
///
class RingBufferHeader {
 public:
  const uint64_t kDataPoolByteSize;
  const uint64_t kDataModMask;

  RingBufferHeader(const RingBufferHeader&) = delete;
  RingBufferHeader(RingBufferHeader&&) = delete;

  // Implementation uses power of 2 arithmetic to avoid costly modulo.
  // So build the largest RingBuffer with size of the smallest power of 2 >=
  // <byte_size>.
  RingBufferHeader(uint64_t min_data_byte_size)
      : kDataPoolByteSize{nextPow2(min_data_byte_size)},
        kDataModMask{kDataPoolByteSize - 1} {
    // Minimum size where implementation of bit shift arithmetic works.
    TP_DCHECK_GE(kDataPoolByteSize, 2)
        << "Minimum supported ringbuffer data size is 2 bytes";
    TP_DCHECK(isPow2(kDataPoolByteSize))
        << kDataPoolByteSize << " is not a power of 2";
    TP_DCHECK_LE(kDataPoolByteSize, std::numeric_limits<int>::max())
        << "Logic piggy-backs read/write size on ints, to be safe forbid"
           " buffer to ever be larger than what an int can hold";
    in_write_tx.clear();
    in_read_tx.clear();
  }

  // Get size that is only guaranteed to be correct when producers and consumers
  // are synchronized.
  size_t usedSizeWeak() const {
    return atomicHead_ - atomicTail_;
  }

  size_t freeSizeWeak() const {
    return kDataPoolByteSize - usedSizeWeak();
  }

  uint64_t readHead() const {
    return atomicHead_;
  }

  uint64_t readTail() const {
    return atomicTail_;
  }

  void incHead(uint64_t inc) {
    atomicHead_ += inc;
  }
  void incTail(uint64_t inc) {
    atomicTail_ += inc;
  }

  void drop() {
    atomicTail_.store(atomicHead_.load());
  }

  // acquired by producers.
  std::atomic_flag in_write_tx;
  // acquired by consumers.
  std::atomic_flag in_read_tx;

 protected:
  // Written by producer.
  std::atomic<uint64_t> atomicHead_{0};
  // Written by consumer.
  std::atomic<uint64_t> atomicTail_{0};

  // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2427.html#atomics.lockfree
  // static_assert(
  //     decltype(atomicHead_)::is_always_lock_free,
  //     "Only lock-free atomics are supported");
  // static_assert(
  //     decltype(atomicTail_)::is_always_lock_free,
  //     "Only lock-free atomics are supported");
};

///
/// Process' view of a ring buffer.
/// This cannot reside in shared memory since it has pointers.
///
class RingBuffer final {
 public:
  RingBuffer(const RingBuffer&) = delete;
  RingBuffer(RingBuffer&&) = delete;

  RingBuffer(
      std::shared_ptr<RingBufferHeader> header,
      std::shared_ptr<uint8_t> data)
      : header_{std::move(header)}, data_{std::move(data)} {
    TP_THROW_IF_NULLPTR(header_) << "Header cannot be nullptr";
    TP_THROW_IF_NULLPTR(data_) << "Data cannot be nullptr";
  }

  const RingBufferHeader& getHeader() const {
    return *header_;
  }

  RingBufferHeader& getHeader() {
    return *header_;
  }

  const uint8_t* getData() const {
    return data_.get();
  }

  uint8_t* getData() {
    return data_.get();
  }

 protected:
  std::shared_ptr<RingBufferHeader> header_;

  // Note: this is a std::shared_ptr<uint8_t[]> semantically.
  // A shared_ptr with array type is supported in C++17 and higher.
  std::shared_ptr<uint8_t> data_;
};

///
/// Ringbuffer wrapper
///
class RingBufferWrapper {
 public:
  RingBufferWrapper(const RingBufferWrapper&) = delete;

  RingBufferWrapper(std::shared_ptr<RingBuffer> rb)
      : rb_{rb}, header_{rb->getHeader()}, data_{rb->getData()} {
    TP_THROW_IF_NULLPTR(rb);
    TP_THROW_IF_NULLPTR(data_);
  }

  auto& getHeader() {
    return header_;
  }

  const auto& getHeader() const {
    return header_;
  }

  auto getRingBuffer() {
    return rb_;
  }

 protected:
  std::shared_ptr<RingBuffer> rb_;
  RingBufferHeader& header_;
  uint8_t* const data_;
  unsigned tx_size_ = 0;
};

} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
