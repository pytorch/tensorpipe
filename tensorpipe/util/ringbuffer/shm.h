#pragma once

#include <tensorpipe/util/ringbuffer/ringbuffer.h>
#include <tensorpipe/util/shm/segment.h>

#include <type_traits>

namespace tensorpipe {
namespace util {
namespace ringbuffer {
namespace shm {

/// Creates ringbuffer on shared memory.
///
/// RingBuffer's data can have any <tensorpipe::util::shm::PageType>
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
template <class TRingBuffer>
std::tuple<int, int, std::shared_ptr<TRingBuffer>> create(
    size_t min_rb_byte_size,
    optional<tensorpipe::util::shm::PageType> data_page_type = nullopt,
    bool perm_write = true) {
  std::shared_ptr<typename TRingBuffer::THeader> header;
  std::shared_ptr<tensorpipe::util::shm::Segment> header_segment;
  std::tie(header, header_segment) =
      tensorpipe::util::shm::Segment::create<typename TRingBuffer::THeader>(
          perm_write,
          tensorpipe::util::shm::PageType::Default,
          min_rb_byte_size);

  std::shared_ptr<uint8_t[]> data;
  std::shared_ptr<tensorpipe::util::shm::Segment> data_segment;
  std::tie(data, data_segment) =
      tensorpipe::util::shm::Segment::create<uint8_t[]>(
          header->kDataPoolByteSize, perm_write, data_page_type);

  return {header_segment->getFd(),
          data_segment->getFd(),
          std::make_shared<TRingBuffer>(std::move(header), std::move(data))};
}

template <class TRingBuffer>
std::shared_ptr<TRingBuffer> load(
    int header_fd,
    int data_fd,
    optional<tensorpipe::util::shm::PageType> data_page_type = nullopt,
    bool perm_write = true) {
  std::shared_ptr<typename TRingBuffer::THeader> header;
  std::shared_ptr<tensorpipe::util::shm::Segment> header_segment;
  std::tie(header, header_segment) =
      tensorpipe::util::shm::Segment::load<typename TRingBuffer::THeader>(
          header_fd, perm_write, tensorpipe::util::shm::PageType::Default);
  constexpr auto kHeaderSize = sizeof(typename TRingBuffer::THeader);
  if (unlikely(kHeaderSize != header_segment->getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Header segment of unexpected size";
  }

  std::shared_ptr<uint8_t[]> data;
  std::shared_ptr<tensorpipe::util::shm::Segment> data_segment;
  std::tie(data, data_segment) =
      tensorpipe::util::shm::Segment::load<uint8_t[]>(
          data_fd, perm_write, data_page_type);
  if (unlikely(header->kDataPoolByteSize != data_segment->getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Data segment of unexpected size";
  }

  return std::make_shared<TRingBuffer>(std::move(header), std::move(data));
}

} // namespace shm
} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
