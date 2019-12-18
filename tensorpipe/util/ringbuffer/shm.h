#pragma once

#include <tensorpipe/util/ringbuffer/ringbuffer.h>
#include <tensorpipe/util/shm/segment.h>

#include <type_traits>

namespace tensorpipe {
namespace util {
namespace ringbuffer {
namespace shm {

namespace {

std::pair<std::string, std::string> segmentNames(const std::string& prefix) {
  return {prefix + "/rb/header", prefix + "/rb/data"};
}

} // namespace

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
auto create(
    const std::string& segment_prefix,
    size_t min_rb_byte_size,
    bool persistent,
    optional<tensorpipe::util::shm::PageType> data_page_type = nullopt,
    bool perm_write = true) {
  std::string header_name;
  std::string data_name;
  std::tie(header_name, data_name) = segmentNames(segment_prefix);
  // if buffer is not persistent, it's underlying shared memory segment will
  // be unlinked with it.
  // Do not link the shared memory segment to a path on creation because
  // the ringbuffer needs to be initialized first.
  const auto link_flags = persistent
      ? tensorpipe::util::shm::CreationMode::None
      : tensorpipe::util::shm::CreationMode::UnlinkOnDestruction;

  std::shared_ptr<typename TRingBuffer::THeader> header;
  std::shared_ptr<tensorpipe::util::shm::Segment> header_segment;
  std::tie(header, header_segment) =
      tensorpipe::util::shm::Segment::create<typename TRingBuffer::THeader>(
          header_name,
          perm_write,
          tensorpipe::util::shm::PageType::Default,
          link_flags,
          min_rb_byte_size);

  std::shared_ptr<uint8_t[]> data;
  std::shared_ptr<tensorpipe::util::shm::Segment> data_segment;
  std::tie(data, data_segment) =
      tensorpipe::util::shm::Segment::create<uint8_t[]>(
          header->kDataPoolByteSize,
          data_name,
          perm_write,
          data_page_type,
          link_flags);

  // Custom destructor to release shared_ptr's captured by lambda.
  auto deleter = [header, data](TRingBuffer* rb) { delete rb; };
  auto rb = std::shared_ptr<TRingBuffer>(
      new TRingBuffer(header.get(), data.get()), deleter);

  // Link to make accessible to others.
  header_segment->link();
  data_segment->link();
  return rb;
}

template <class TRingBuffer>
std::shared_ptr<TRingBuffer> load(
    const std::string& segment_prefix,
    optional<tensorpipe::util::shm::PageType> data_page_type = nullopt,
    bool perm_write = true) {
  std::string header_name;
  std::string data_name;
  std::tie(header_name, data_name) = segmentNames(segment_prefix);

  std::shared_ptr<typename TRingBuffer::THeader> header;
  std::shared_ptr<tensorpipe::util::shm::Segment> header_segment;
  std::tie(header, header_segment) =
      tensorpipe::util::shm::Segment::load<typename TRingBuffer::THeader>(
          header_name, perm_write, tensorpipe::util::shm::PageType::Default);
  constexpr auto kHeaderSize = sizeof(typename TRingBuffer::THeader);
  if (unlikely(kHeaderSize != header_segment->getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Header segment of unexpected size";
  }

  std::shared_ptr<uint8_t[]> data;
  std::shared_ptr<tensorpipe::util::shm::Segment> data_segment;
  std::tie(data, data_segment) =
      tensorpipe::util::shm::Segment::load<uint8_t[]>(
          data_name, perm_write, data_page_type);
  if (unlikely(header->kDataPoolByteSize != data_segment->getSize())) {
    TP_THROW_SYSTEM(EPERM) << "Data segment of unexpected size";
  }

  // Custom destructor to release shared_ptr's captured by lambda.
  auto deleter = [header, data](TRingBuffer* rb) { delete rb; };
  return std::shared_ptr<TRingBuffer>(
      new TRingBuffer(header.get(), data.get()), deleter);
}

// This function Keep template parameter because we may need to make the segment
// names depend on the ringbuffer type.
template <class TRingBuffer>
inline void unlink(const std::string& segment_prefix) {
  std::string header_name;
  std::string data_name;
  std::tie(header_name, data_name) = segmentNames(segment_prefix);
  tensorpipe::util::shm::Segment::unlink(header_name);
  tensorpipe::util::shm::Segment::unlink(data_name);
}

} // namespace shm
} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
