/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/util/shm/segment.h>

#include <fcntl.h>
#include <linux/mman.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <sstream>
#include <thread>
#include <tuple>

namespace tensorpipe {
namespace util {
namespace shm {

namespace {

// Default base path for all segments created.
constexpr const char* kBasePath = "/dev/shm";

Fd createShmFd() {
  int flags = O_TMPFILE | O_EXCL | O_RDWR | O_CLOEXEC;
  int fd = ::open(kBasePath, flags, 0);
  TP_THROW_SYSTEM_IF(fd == -1, errno)
      << "Failed to open shared memory file descriptor at " << kBasePath;
  return Fd(fd);
}

/// Choose a reasonable page size for a given size.
/// This very opinionated choice of "reasonable" aims to
/// keep wasted memory low.
// XXX: Lots of memory could be wasted if size is slightly larger than
// page size, handle that case.
constexpr PageType getDefaultPageType(uint64_t size) {
  constexpr uint64_t mb = 1024ull * 1024ull;
  constexpr uint64_t gb = 1024ull * mb;

  if (size >= (15ul * gb) / 16ul) {
    // At least 15/16 of a 1GB page (at most 64 MB wasted).
    return PageType::HugeTLB_1GB;
  } else if (size >= ((2ul * mb) * 3ul) / 4ul) {
    // At least 3/4 of a 2MB page (at most 512 KB wasteds)
    return PageType::HugeTLB_2MB;
  } else {
    // No Huge TLB page, use Default (4KB for x86).
    return PageType::Default;
  }
}

MmappedPtr mmapShmFd(
    int fd,
    size_t byteSize,
    bool permWrite,
    optional<PageType> pageType) {
#ifdef MAP_SHARED_VALIDATE
  int flags = MAP_SHARED | MAP_SHARED_VALIDATE;
#else

#warning \
    "this version of libc doesn't define MAP_SHARED_VALIDATE, \
update to obtain the latest correctness checks."

  int flags = MAP_SHARED;
#endif
  // Note that in x86 PROT_WRITE implies PROT_READ so there is no
  // point on allowing read protection to be specified.
  // Currently no handling PROT_EXEC because there is no use case for it.
  int prot = PROT_READ;
  if (permWrite) {
    prot |= PROT_WRITE;
  }

  // FIXME mmap seems to fail with EINVAL if it cannot use hugepages when asked
  // to do so. We could try with hugepages and, if it fails, try again without.
  // But this adds complexity and I first want to know how much performance we
  // gain through hugepages but so far I haven't been able to get them to work.
  // switch (page_type.value_or(getDefaultPageType(byte_size))) {
  //   case PageType::Default:
  //     break;
  //   case PageType::HugeTLB_2MB:
  //     flags |= MAP_HUGETLB | MAP_HUGE_2MB;
  //     break;
  //   case PageType::HugeTLB_1GB:
  //     flags |= MAP_HUGETLB | MAP_HUGE_1GB;
  //     break;
  // }

  return MmappedPtr(byteSize, prot, flags, fd);
}

} // namespace

Segment::Segment(size_t byteSize, bool permWrite, optional<PageType> pageType)
    : fd_(createShmFd()) {
  // grow size to contain byte_size bytes.
  off_t len = static_cast<off_t>(byteSize);
  int ret = ::fallocate(fd_.fd(), 0, 0, len);
  TP_THROW_SYSTEM_IF(ret == -1, errno)
      << "Error while allocating " << byteSize << " bytes in shared memory";

  ptr_ = mmapShmFd(fd_.fd(), byteSize, permWrite, pageType);
}

Segment::Segment(Fd fd, bool permWrite, optional<PageType> pageType)
    : fd_(std::move(fd)) {
  // Load whole file. Use fstat to obtain size.
  struct stat sb;
  int ret = ::fstat(fd_.fd(), &sb);
  TP_THROW_SYSTEM_IF(ret == -1, errno)
      << "Error while fstat shared memory file";
  size_t byteSize = static_cast<size_t>(sb.st_size);

  ptr_ = mmapShmFd(fd_.fd(), byteSize, permWrite, pageType);
}

} // namespace shm
} // namespace util
} // namespace tensorpipe
