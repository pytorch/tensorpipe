/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/shm_segment.h>

#include <fcntl.h>
#include <linux/mman.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <sstream>
#include <thread>
#include <tuple>

namespace tensorpipe {

namespace {

// Our goal is to obtain a file descriptor that is backed by a region of memory.
// (We need an fd so we can pass it over a UNIX domain socket). We support two
// ways of doing so:
// - The memfd_create syscall, which does exactly what we need. Unfortunately
//   it was added in a recent-ish kernel and an even more recent glibc version.
// - As a fallback for older systems, we open a file in the /dev/shm directory,
//   which we expect to be a mountpoint of tmpfs type. We open it with O_TMPFILE
//   so it remains unnamed, which won't appear in the directory and can't thus
//   be opened by other processes and will be automatically cleaned up when we
//   exit. This method has some issues, as it depends on the availability of
//   /dev/shm and is capped to the size of that mountpoint (rather than the
//   total memory of the system), which are especially problematic in Docker.
// FIXME O_TMPFILE is also not that old, and some users have reported issues due
// to it. We could add a third method as a further fallback.

// Name to give to the memfds. This is just displayed when inspecting the file
// descriptor in /proc/self/fd to aid debugging, and doesn't have to be unique.
constexpr const char* kMemfdName = "tensorpipe_shm";

std::tuple<Error, Fd> createMemfd() {
  // We don't want to use the ::memfd_create function directly as it's harder to
  // detect its availability (we'd need to perform a feature check in CMake and
  // inject the result as a preprocessor flag) and because it would cause us to
  // link against glibc 2.27. PyTorch aims to support the manylinux2014 platform
  // (one of the standard platforms defined by Python for PyPI/pip), which has
  // glibc 2.17. Thus instead we issue the syscall directly, skipping the glibc
  // wrapper.
#ifdef SYS_memfd_create
  // We want to pass the MFD_CLOEXEC flag, but we can't rely on glibc exposing
  // it, thus we hardcode its value, which is equal to 1.
  int fd = static_cast<int>(::syscall(
      SYS_memfd_create,
      static_cast<const char*>(kMemfdName),
      static_cast<unsigned int>(1)));
  if (fd < 0) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "memfd_create", errno), Fd());
  }
  return std::make_tuple(Error::kSuccess, Fd(fd));
#else // SYS_memfd_create
  return std::make_tuple(
      TP_CREATE_ERROR(SystemError, "memfd_create", ENOSYS), Fd());
#endif // SYS_memfd_create
}

// Default base path for all segments created.
constexpr const char* kBasePath = "/dev/shm";

std::tuple<Error, Fd> openTmpfileInDevShm() {
  int flags = O_TMPFILE | O_EXCL | O_RDWR | O_CLOEXEC;
  int fd = ::open(kBasePath, flags, 0);
  if (fd < 0) {
    return std::make_tuple(TP_CREATE_ERROR(SystemError, "open", errno), Fd());
  }

  return std::make_tuple(Error::kSuccess, Fd(fd));
}

std::tuple<Error, Fd> createShmFd() {
  Error error;
  Fd fd;
  std::tie(error, fd) = createMemfd();
  if (error && error.isOfType<SystemError>() &&
      error.castToType<SystemError>()->errorCode() == ENOSYS) {
    std::tie(error, fd) = openTmpfileInDevShm();
  }
  return std::make_tuple(std::move(error), std::move(fd));
}

/// Choose a reasonable page size for a given size.
/// This very opinionated choice of "reasonable" aims to
/// keep wasted memory low.
// XXX: Lots of memory could be wasted if size is slightly larger than
// page size, handle that case.
constexpr ShmSegment::PageType getDefaultPageType(uint64_t size) {
  constexpr uint64_t mb = 1024ull * 1024ull;
  constexpr uint64_t gb = 1024ull * mb;

  if (size >= (15ul * gb) / 16ul) {
    // At least 15/16 of a 1GB page (at most 64 MB wasted).
    return ShmSegment::PageType::HugeTLB_1GB;
  } else if (size >= ((2ul * mb) * 3ul) / 4ul) {
    // At least 3/4 of a 2MB page (at most 512 KB wasteds)
    return ShmSegment::PageType::HugeTLB_2MB;
  } else {
    // No Huge TLB page, use Default (4KB for x86).
    return ShmSegment::PageType::Default;
  }
}

#ifdef MAP_SHARED_VALIDATE
bool isMapSharedValidateSupported() {
  static bool rc = []() {
    // If MAP_SHARED_VALIDATE is supported by kernel,
    // errno should be EOPNOTSUPP, if not EINVAL
    void* ptr = mmap(nullptr, 8, 0, 0x800000 | MAP_SHARED_VALIDATE, 0, 0);
    return ptr == MAP_FAILED && errno == EOPNOTSUPP;
  }();
  return rc;
}
#endif

std::tuple<Error, MmappedPtr> mmapShmFd(
    int fd,
    size_t byteSize,
    bool permWrite,
    optional<ShmSegment::PageType> pageType) {
#ifdef MAP_SHARED_VALIDATE
  int flags = isMapSharedValidateSupported() ? MAP_SHARED_VALIDATE : MAP_SHARED;
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
  //   case ShmSegment::PageType::Default:
  //     break;
  //   case ShmSegment::PageType::HugeTLB_2MB:
  //     flags |= MAP_HUGETLB | MAP_HUGE_2MB;
  //     break;
  //   case ShmSegment::PageType::HugeTLB_1GB:
  //     flags |= MAP_HUGETLB | MAP_HUGE_1GB;
  //     break;
  // }

  return MmappedPtr::create(byteSize, prot, flags, fd);
}

} // namespace

ShmSegment::ShmSegment(Fd fd, MmappedPtr ptr)
    : fd_(std::move(fd)), ptr_(std::move(ptr)) {}

std::tuple<Error, ShmSegment> ShmSegment::alloc(
    size_t byteSize,
    bool permWrite,
    optional<ShmSegment::PageType> pageType) {
  Error error;
  Fd fd;
  std::tie(error, fd) = createShmFd();
  if (error) {
    return std::make_tuple(std::move(error), ShmSegment());
  }

  // grow size to contain byte_size bytes.
  off_t len = static_cast<off_t>(byteSize);
  int ret = ::fallocate(fd.fd(), 0, 0, len);
  if (ret < 0) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "fallocate", errno), ShmSegment());
  }

  MmappedPtr ptr;
  std::tie(error, ptr) = mmapShmFd(fd.fd(), byteSize, permWrite, pageType);
  if (error) {
    return std::make_tuple(std::move(error), ShmSegment());
  }

  return std::make_tuple(
      Error::kSuccess, ShmSegment(std::move(fd), std::move(ptr)));
}

std::tuple<Error, ShmSegment> ShmSegment::access(
    Fd fd,
    bool permWrite,
    optional<ShmSegment::PageType> pageType) {
  // Load whole file. Use fstat to obtain size.
  struct stat sb;
  int ret = ::fstat(fd.fd(), &sb);
  if (ret < 0) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "fstat", errno), ShmSegment());
  }
  size_t byteSize = static_cast<size_t>(sb.st_size);

  Error error;
  MmappedPtr ptr;
  std::tie(error, ptr) = mmapShmFd(fd.fd(), byteSize, permWrite, pageType);
  if (error) {
    return std::make_tuple(std::move(error), ShmSegment());
  }

  return std::make_tuple(
      Error::kSuccess, ShmSegment(std::move(fd), std::move(ptr)));
}

} // namespace tensorpipe
