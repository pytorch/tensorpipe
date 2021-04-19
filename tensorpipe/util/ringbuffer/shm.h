/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/fd.h>
#include <tensorpipe/util/ringbuffer/ringbuffer.h>
#include <tensorpipe/util/shm/segment.h>

namespace tensorpipe {
namespace util {
namespace ringbuffer {
namespace shm {

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
std::tuple<Error, util::shm::Segment, util::shm::Segment, RingBuffer<NumRoles>>
create(
    size_t minRbByteSize,
    optional<util::shm::PageType> dataPageType = nullopt,
    bool permWrite = true) {
  Error error;
  util::shm::Segment headerSegment;
  RingBufferHeader<NumRoles>* header;
  std::tie(error, headerSegment, header) =
      util::shm::Segment::create<RingBufferHeader<NumRoles>>(
          permWrite, util::shm::PageType::Default, minRbByteSize);
  if (error) {
    return std::make_tuple(
        std::move(error),
        util::shm::Segment(),
        util::shm::Segment(),
        RingBuffer<NumRoles>());
  }

  util::shm::Segment dataSegment;
  uint8_t* data;
  std::tie(error, dataSegment, data) = util::shm::Segment::create<uint8_t[]>(
      header->kDataPoolByteSize, permWrite, dataPageType);
  if (error) {
    return std::make_tuple(
        std::move(error),
        util::shm::Segment(),
        util::shm::Segment(),
        RingBuffer<NumRoles>());
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
std::tuple<Error, util::shm::Segment, util::shm::Segment, RingBuffer<NumRoles>>
load(
    Fd headerFd,
    Fd dataFd,
    optional<util::shm::PageType> dataPageType = nullopt,
    bool permWrite = true) {
  Error error;
  util::shm::Segment headerSegment;
  RingBufferHeader<NumRoles>* header;
  std::tie(error, headerSegment, header) =
      util::shm::Segment::load<RingBufferHeader<NumRoles>>(
          std::move(headerFd), permWrite, util::shm::PageType::Default);
  if (error) {
    return std::make_tuple(
        std::move(error),
        util::shm::Segment(),
        util::shm::Segment(),
        RingBuffer<NumRoles>());
  }
  constexpr auto kHeaderSize = sizeof(RingBufferHeader<NumRoles>);
  if (unlikely(kHeaderSize != headerSegment.getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Header segment of unexpected size";
  }

  util::shm::Segment dataSegment;
  uint8_t* data;
  std::tie(error, dataSegment, data) = util::shm::Segment::load<uint8_t[]>(
      std::move(dataFd), permWrite, dataPageType);
  if (error) {
    return std::make_tuple(
        std::move(error),
        util::shm::Segment(),
        util::shm::Segment(),
        RingBuffer<NumRoles>());
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

} // namespace shm
} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
