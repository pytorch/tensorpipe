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
    size_t min_rb_byte_size,
    optional<util::shm::PageType> data_page_type,
    bool perm_write) {
  util::shm::Segment header_segment;
  RingBufferHeader* header;
  std::tie(header_segment, header) =
      util::shm::Segment::create<RingBufferHeader>(
          perm_write, util::shm::PageType::Default, min_rb_byte_size);

  util::shm::Segment data_segment;
  uint8_t* data;
  std::tie(data_segment, data) = util::shm::Segment::create<uint8_t[]>(
      header->kDataPoolByteSize, perm_write, data_page_type);

  // Note: cannot use implicit construction from initializer list on GCC 5.5:
  // "converting to XYZ from initializer list would use explicit constructor".
  return std::make_tuple(
      std::move(header_segment),
      std::move(data_segment),
      RingBuffer(header, data));
}

std::tuple<util::shm::Segment, util::shm::Segment, RingBuffer> load(
    Fd header_fd,
    Fd data_fd,
    optional<util::shm::PageType> data_page_type,
    bool perm_write) {
  util::shm::Segment header_segment;
  RingBufferHeader* header;
  std::tie(header_segment, header) = util::shm::Segment::load<RingBufferHeader>(
      std::move(header_fd), perm_write, util::shm::PageType::Default);
  constexpr auto kHeaderSize = sizeof(RingBufferHeader);
  if (unlikely(kHeaderSize != header_segment.getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Header segment of unexpected size";
  }

  util::shm::Segment data_segment;
  uint8_t* data;
  std::tie(data_segment, data) = util::shm::Segment::load<uint8_t[]>(
      std::move(data_fd), perm_write, data_page_type);
  if (unlikely(header->kDataPoolByteSize != data_segment.getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Data segment of unexpected size";
  }

  return std::make_tuple(
      std::move(header_segment),
      std::move(data_segment),
      RingBuffer(header, data));
}

} // namespace shm
} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
