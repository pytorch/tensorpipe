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

std::tuple<int, int, std::shared_ptr<RingBuffer>> create(
    size_t min_rb_byte_size,
    optional<tensorpipe::util::shm::PageType> data_page_type,
    bool perm_write) {
  std::shared_ptr<RingBufferHeader> header;
  std::shared_ptr<tensorpipe::util::shm::Segment> header_segment;
  std::tie(header, header_segment) =
      tensorpipe::util::shm::Segment::create<RingBufferHeader>(
          perm_write,
          tensorpipe::util::shm::PageType::Default,
          min_rb_byte_size);

  std::shared_ptr<uint8_t> data;
  std::shared_ptr<tensorpipe::util::shm::Segment> data_segment;
  std::tie(data, data_segment) =
      tensorpipe::util::shm::Segment::create<uint8_t[]>(
          header->kDataPoolByteSize, perm_write, data_page_type);

  // Note: cannot use implicit construction from initializer list on GCC 5.5:
  // "converting to XYZ from initializer list would use explicit constructor".
  return std::make_tuple(
      header_segment->getFd(),
      data_segment->getFd(),
      std::make_shared<RingBuffer>(std::move(header), std::move(data)));
}

std::shared_ptr<RingBuffer> load(
    int header_fd,
    int data_fd,
    optional<tensorpipe::util::shm::PageType> data_page_type,
    bool perm_write) {
  std::shared_ptr<RingBufferHeader> header;
  std::shared_ptr<tensorpipe::util::shm::Segment> header_segment;
  std::tie(header, header_segment) =
      tensorpipe::util::shm::Segment::load<RingBufferHeader>(
          header_fd, perm_write, tensorpipe::util::shm::PageType::Default);
  constexpr auto kHeaderSize = sizeof(RingBufferHeader);
  if (unlikely(kHeaderSize != header_segment->getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Header segment of unexpected size";
  }

  std::shared_ptr<uint8_t> data;
  std::shared_ptr<tensorpipe::util::shm::Segment> data_segment;
  std::tie(data, data_segment) =
      tensorpipe::util::shm::Segment::load<uint8_t[]>(
          data_fd, perm_write, data_page_type);
  if (unlikely(header->kDataPoolByteSize != data_segment->getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Data segment of unexpected size";
  }

  return std::make_shared<RingBuffer>(std::move(header), std::move(data));
}

} // namespace shm
} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
