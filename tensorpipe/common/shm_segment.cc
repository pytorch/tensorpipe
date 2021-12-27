/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
  // it, thus we redefine its value if needed.
#ifndef MFD_CLOEXEC
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/memfd.h
#define MFD_CLOEXEC 0x0001U
#endif
  int fd = static_cast<int>(::syscall(
      SYS_memfd_create,
      static_cast<const char*>(kMemfdName),
      static_cast<unsigned int>(MFD_CLOEXEC)));
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
  // Some users are compiling on old pre-3.11 kernels. We'd like our backends to
  // only depend on runtime capabilities, and not on compile-time ones, hence we
  // "polyfill" the flag so the build will pass and we'll get a runtime error.
#ifndef O_TMPFILE
// https://github.com/torvalds/linux/blob/master/include/uapi/asm-generic/fcntl.h
#define O_TMPFILE (020000000 | 00200000)
#endif
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

std::tuple<Error, MmappedPtr> mmapShmFd(int fd, size_t byteSize) {
  int flags = MAP_SHARED;
  int prot = PROT_READ | PROT_WRITE;
  return MmappedPtr::create(byteSize, prot, flags, fd);
}

} // namespace

ShmSegment::ShmSegment(Fd fd, MmappedPtr ptr)
    : fd_(std::move(fd)), ptr_(std::move(ptr)) {}

std::tuple<Error, ShmSegment> ShmSegment::alloc(size_t byteSize) {
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
  std::tie(error, ptr) = mmapShmFd(fd.fd(), byteSize);
  if (error) {
    return std::make_tuple(std::move(error), ShmSegment());
  }

  return std::make_tuple(
      Error::kSuccess, ShmSegment(std::move(fd), std::move(ptr)));
}

std::tuple<Error, ShmSegment> ShmSegment::access(Fd fd) {
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
  std::tie(error, ptr) = mmapShmFd(fd.fd(), byteSize);
  if (error) {
    return std::make_tuple(std::move(error), ShmSegment());
  }

  return std::make_tuple(
      Error::kSuccess, ShmSegment(std::move(fd), std::move(ptr)));
}

} // namespace tensorpipe
