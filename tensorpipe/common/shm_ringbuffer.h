/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/fd.h>
#include <tensorpipe/common/ringbuffer.h>
#include <tensorpipe/common/shm_segment.h>

namespace tensorpipe {

/// Creates ringbuffer on shared memory.
///
/// RingBuffer's data can have any <util::shm::PageType>
/// (e.g. 4KB or a HugeTLB Page of 2MB or 1GB). If  <data_page_type> is not
/// provided, then choose the largest page that would result in
/// close to full occupancy.
///
/// If <persistent>, the shared memory will not be unlinked
/// when RingBuffer is destroyed.
///
/// <min_rb_byte_size> is the minimum size of the data section
/// of a RingBuffer (or each CPU's RingBuffer).
///
template <int NumRoles>
std::tuple<Error, ShmSegment, ShmSegment, RingBuffer<NumRoles>>
createShmRingBuffer(
    size_t minRbByteSize,
    optional<ShmSegment::PageType> dataPageType = nullopt,
    bool permWrite = true) {
  Error error;
  ShmSegment headerSegment;
  RingBufferHeader<NumRoles>* header;
  std::tie(error, headerSegment, header) =
      ShmSegment::create<RingBufferHeader<NumRoles>>(
          permWrite, ShmSegment::PageType::Default, minRbByteSize);
  if (error) {
    return std::make_tuple(
        std::move(error), ShmSegment(), ShmSegment(), RingBuffer<NumRoles>());
  }

  ShmSegment dataSegment;
  uint8_t* data;
  std::tie(error, dataSegment, data) = ShmSegment::create<uint8_t[]>(
      header->kDataPoolByteSize, permWrite, dataPageType);
  if (error) {
    return std::make_tuple(
        std::move(error), ShmSegment(), ShmSegment(), RingBuffer<NumRoles>());
  }

  // Note: cannot use implicit construction from initializer list on GCC 5.5:
  // "converting to XYZ from initializer list would use explicit constructor".
  return std::make_tuple(
      Error::kSuccess,
      std::move(headerSegment),
      std::move(dataSegment),
      RingBuffer<NumRoles>(header, data));
}

template <int NumRoles>
std::tuple<Error, ShmSegment, ShmSegment, RingBuffer<NumRoles>>
loadShmRingBuffer(
    Fd headerFd,
    Fd dataFd,
    optional<ShmSegment::PageType> dataPageType = nullopt,
    bool permWrite = true) {
  Error error;
  ShmSegment headerSegment;
  RingBufferHeader<NumRoles>* header;
  std::tie(error, headerSegment, header) =
      ShmSegment::load<RingBufferHeader<NumRoles>>(
          std::move(headerFd), permWrite, ShmSegment::PageType::Default);
  if (error) {
    return std::make_tuple(
        std::move(error), ShmSegment(), ShmSegment(), RingBuffer<NumRoles>());
  }
  constexpr auto kHeaderSize = sizeof(RingBufferHeader<NumRoles>);
  if (unlikely(kHeaderSize != headerSegment.getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Header segment of unexpected size";
  }

  ShmSegment dataSegment;
  uint8_t* data;
  std::tie(error, dataSegment, data) =
      ShmSegment::load<uint8_t[]>(std::move(dataFd), permWrite, dataPageType);
  if (error) {
    return std::make_tuple(
        std::move(error), ShmSegment(), ShmSegment(), RingBuffer<NumRoles>());
  }
  if (unlikely(header->kDataPoolByteSize != dataSegment.getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Data segment of unexpected size";
  }

  return std::make_tuple(
      Error::kSuccess,
      std::move(headerSegment),
      std::move(dataSegment),
      RingBuffer<NumRoles>(header, data));
}

} // namespace tensorpipe
