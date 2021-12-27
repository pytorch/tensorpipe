/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

///
/// RingBufferHeader contains the head, tail and other control information
/// of the RingBuffer.
///
/// <kMinByteSize_> is the minimum byte size of the circular buffer. The actual
/// size is the smallest power of 2 larger than kMinByteSize_. Enforcing the
/// size to be a power of two avoids costly division/modulo operations.
///
template <int NumRoles>
class RingBufferHeader {
 public:
  static_assert(NumRoles > 0, "");
  const uint64_t kDataPoolByteSize;
  const uint64_t kDataModMask;

  RingBufferHeader(const RingBufferHeader&) = delete;
  RingBufferHeader(RingBufferHeader&&) = delete;

  // Implementation uses power of 2 arithmetic to avoid costly modulo.
  // So build the largest RingBuffer with size of the smallest power of 2 >=
  // <byte_size>.
  explicit RingBufferHeader(uint64_t minDataByteSize)
      : kDataPoolByteSize{nextPow2(minDataByteSize)},
        kDataModMask{kDataPoolByteSize - 1} {
    // Minimum size where implementation of bit shift arithmetic works.
    TP_DCHECK_GE(kDataPoolByteSize, 2)
        << "Minimum supported ringbuffer data size is 2 bytes";
    TP_DCHECK(isPow2(kDataPoolByteSize))
        << kDataPoolByteSize << " is not a power of 2";
    TP_DCHECK_LE(kDataPoolByteSize, std::numeric_limits<int>::max())
        << "Logic piggy-backs read/write size on ints, to be safe forbid"
           " buffer to ever be larger than what an int can hold";
    for (int roleIdx = 0; roleIdx < NumRoles; ++roleIdx) {
      inTx_[roleIdx].clear();
      markers_[roleIdx] = 0;
    }
  }

  // Being in a transaction (either a read or a write one) gives a user of the
  // ringbuffer (either a consumer or a producer, respectively) the right to
  // read the head and tail and to modify the one they are responsible for (the
  // tail and the head, respectively). Accessing the head or tail outside of a
  // transaction could lead to races. This also means we need memory barriers
  // around a transaction, to make sure side-effects of other users are visible
  // upon entering and our side effects become visible to others upon exiting.
  // We also must prevent the compiler from reordering memory accesses. Failure
  // to do so may result in our reads of head/tail to look like they occurred
  // before we entered the transaction, and writes to them to look like they
  // occurred after we exited it. In order to get the desired behavior, we use
  // the acquire memory order when starting a transaction (which means no later
  // memory access can be moved before it) and the release memory order when
  // ending it (no earlier memory access can be moved after it).

  template <int RoleIdx>
  [[nodiscard]] bool beginTransaction() {
    static_assert(0 <= RoleIdx && RoleIdx < NumRoles, "");
    return inTx_[RoleIdx].test_and_set(std::memory_order_acquire);
  }

  template <int RoleIdx>
  void endTransaction() {
    static_assert(0 <= RoleIdx && RoleIdx < NumRoles, "");
    inTx_[RoleIdx].clear(std::memory_order_release);
  }

  // Reading the head and tail is what gives a user of the ringbuffer (either a
  // consumer or a producer) the right to access the buffer's contents: the
  // producer can write on [head, tail) (modulo the size), the consumer can read
  // from [tail, head). And, when the producer increases the head, or when the
  // consumer increases the tail, they give users of the opposite type the right
  // to access some of the memory that was previously under their control. Thus,
  // just like we do for the transactions, we need memory barriers around reads
  // and writes to the head and tail, with the same reasoning for memory orders.

  template <int RoleIdx>
  uint64_t readMarker() const {
    static_assert(0 <= RoleIdx && RoleIdx < NumRoles, "");
    return markers_[RoleIdx].load(std::memory_order_acquire);
  }

  template <int RoleIdx>
  void incMarker(uint64_t inc) {
    static_assert(0 <= RoleIdx && RoleIdx < NumRoles, "");
    markers_[RoleIdx].fetch_add(inc, std::memory_order_release);
  }

 protected:
  std::array<std::atomic_flag, NumRoles> inTx_;
  std::array<std::atomic<uint64_t>, NumRoles> markers_;

  // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2427.html#atomics.lockfree
  // static_assert(
  //     decltype(markers_)::value_type::is_always_lock_free,
  //     "Only lock-free atomics are supported");
};

///
/// Process' view of a ring buffer.
/// This cannot reside in shared memory since it has pointers.
///
template <int NumRoles>
class RingBuffer final {
 public:
  RingBuffer() = default;

  RingBuffer(RingBufferHeader<NumRoles>* header, uint8_t* data)
      : header_(header), data_(data) {
    TP_THROW_IF_NULLPTR(header_) << "Header cannot be nullptr";
    TP_THROW_IF_NULLPTR(data_) << "Data cannot be nullptr";
  }

  const RingBufferHeader<NumRoles>& getHeader() const {
    return *header_;
  }

  RingBufferHeader<NumRoles>& getHeader() {
    return *header_;
  }

  const uint8_t* getData() const {
    return data_;
  }

  uint8_t* getData() {
    return data_;
  }

 protected:
  RingBufferHeader<NumRoles>* header_ = nullptr;
  uint8_t* data_ = nullptr;
};

} // namespace tensorpipe
