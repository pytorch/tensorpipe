/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/mman.h>

#include <memory>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {

class MmappedPtr {
  MmappedPtr(uint8_t* ptr, size_t length) {
    ptr_ = decltype(ptr_)(ptr, Deleter{length});
  }

 public:
  MmappedPtr() = default;

  static std::tuple<Error, MmappedPtr> create(
      size_t length,
      int prot,
      int flags,
      int fd) {
    void* ptr;
    ptr = ::mmap(nullptr, length, prot, flags, fd, 0);
    if (ptr == MAP_FAILED) {
      return std::make_tuple(
          TP_CREATE_ERROR(SystemError, "mmap", errno), MmappedPtr());
    }
    return std::make_tuple(
        Error::kSuccess, MmappedPtr(reinterpret_cast<uint8_t*>(ptr), length));
  }

  uint8_t* ptr() {
    return ptr_.get();
  }

  const uint8_t* ptr() const {
    return ptr_.get();
  }

  size_t getLength() const {
    return ptr_.get_deleter().length;
  }

  void reset() {
    ptr_.reset();
  }

 private:
  struct Deleter {
    size_t length;

    void operator()(void* ptr) {
      int ret = ::munmap(ptr, length);
      TP_THROW_SYSTEM_IF(ret != 0, errno);
    }
  };

  std::unique_ptr<uint8_t[], Deleter> ptr_{nullptr, Deleter{}};
};

} // namespace tensorpipe
