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

[[nodiscard]] int createShmFd() {
  int flags = O_TMPFILE | O_EXCL | O_RDWR | O_CLOEXEC;
  int fd = ::open(Segment::kBasePath, flags, 0);
  if (fd == -1) {
    TP_THROW_SYSTEM(errno) << "Failed to open shared memory file descriptor "
                           << "at " << Segment::kBasePath;
  }
  return fd;
}

// if <byte_size> == 0, map the whole file.
void* mmapShmFd(int fd, size_t byte_size, bool perm_write, PageType page_type) {
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
  if (perm_write) {
    prot |= PROT_WRITE;
  }

  switch (page_type) {
    case PageType::Default:
      break;
    case PageType::HugeTLB_2MB:
      prot |= MAP_HUGETLB | MAP_HUGE_2MB;
      break;
    case PageType::HugeTLB_1GB:
      prot |= MAP_HUGETLB | MAP_HUGE_1GB;
      break;
  }

  void* shm = ::mmap(nullptr, byte_size, prot, flags, fd, 0);
  if (shm == MAP_FAILED) {
    TP_THROW_SYSTEM(errno) << "Failed to mmap memory of size: " << byte_size;
  }

  return shm;
}

} // namespace

// Mention static constexpr char to export the symbol.
constexpr char Segment::kBasePath[];

Segment::Segment(
    size_t byte_size,
    bool perm_write,
    optional<PageType> page_type)
    : fd_{createShmFd()}, byte_size_{byte_size}, base_ptr_{nullptr} {
  // grow size to contain byte_size bytes.
  off_t len = static_cast<off_t>(byte_size_);
  int ret = ::fallocate(fd_, 0, 0, len);
  if (ret == -1) {
    TP_THROW_SYSTEM(errno) << "Error while allocating " << byte_size_
                           << " bytes in shared memory";
  }

  mmap(perm_write, page_type);
}

Segment::Segment(int fd, bool perm_write, optional<PageType> page_type)
    : fd_{fd}, base_ptr_{nullptr} {
  // Load whole file. Use fstat to obtain size.
  struct stat sb;
  int ret = ::fstat(fd_, &sb);
  if (ret == -1) {
    TP_THROW_SYSTEM(errno) << "Error while fstat shared memory file.";
  }
  this->byte_size_ = static_cast<size_t>(sb.st_size);

  mmap(perm_write, page_type);
}

void Segment::mmap(bool perm_write, optional<PageType> page_type) {
  // If page_type is not set, use the default.
  page_type_ = page_type.value_or(getDefaultPageType(this->byte_size_));
  this->base_ptr_ = mmapShmFd(fd_, byte_size_, perm_write, page_type_);
}

Segment::~Segment() {
  int ret = munmap(base_ptr_, byte_size_);
  if (ret == -1) {
    TP_LOG_ERROR() << "Error while munmapping shared memory segment. Error: "
                   << toErrorCode(errno).message();
  }
  if (0 > fd_) {
    TP_LOG_ERROR() << "Attempt to destroy segment with negative file "
                   << "descriptor";
    return;
  }
  ::close(fd_);
}

} // namespace shm
} // namespace util
} // namespace tensorpipe
