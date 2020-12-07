/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/util/ringbuffer/shm.h>

namespace tensorpipe {
namespace util {
namespace ringbuffer {
namespace shm {

std::tuple<util::shm::Segment, util::shm::Segment, RingBuffer> create(
    size_t minRbByteSize,
    optional<util::shm::PageType> dataPageType,
    bool permWrite) {
  util::shm::Segment headerSegment;
  RingBufferHeader* header;
  std::tie(headerSegment, header) =
      util::shm::Segment::create<RingBufferHeader>(
          permWrite, util::shm::PageType::Default, minRbByteSize);

  util::shm::Segment dataSegment;
  uint8_t* data;
  std::tie(dataSegment, data) = util::shm::Segment::create<uint8_t[]>(
      header->kDataPoolByteSize, permWrite, dataPageType);

  // Note: cannot use implicit construction from initializer list on GCC 5.5:
  // "converting to XYZ from initializer list would use explicit constructor".
  return std::make_tuple(
      std::move(headerSegment),
      std::move(dataSegment),
      RingBuffer(header, data));
}

std::tuple<util::shm::Segment, util::shm::Segment, RingBuffer> load(
    Fd headerFd,
    Fd dataFd,
    optional<util::shm::PageType> dataPageType,
    bool permWrite) {
  util::shm::Segment headerSegment;
  RingBufferHeader* header;
  std::tie(headerSegment, header) = util::shm::Segment::load<RingBufferHeader>(
      std::move(headerFd), permWrite, util::shm::PageType::Default);
  constexpr auto kHeaderSize = sizeof(RingBufferHeader);
  if (unlikely(kHeaderSize != headerSegment.getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Header segment of unexpected size";
  }

  util::shm::Segment dataSegment;
  uint8_t* data;
  std::tie(dataSegment, data) = util::shm::Segment::load<uint8_t[]>(
      std::move(dataFd), permWrite, dataPageType);
  if (unlikely(header->kDataPoolByteSize != dataSegment.getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Data segment of unexpected size";
  }

  return std::make_tuple(
      std::move(headerSegment),
      std::move(dataSegment),
      RingBuffer(header, data));
}

} // namespace shm
} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
