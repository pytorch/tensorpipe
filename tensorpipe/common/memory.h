/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/mman.h>

#include <memory>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {

class MmappedPtr {
 public:
  MmappedPtr() = default;

  MmappedPtr(size_t length, int prot, int flags, int fd) {
    void* ptr;
    ptr = ::mmap(nullptr, length, prot, flags, fd, 0);
    TP_THROW_SYSTEM_IF(ptr == MAP_FAILED, errno);
    ptr_ = decltype(ptr_)(reinterpret_cast<uint8_t*>(ptr), Deleter{length});
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
    size_t length = 0;

    void operator()(void* ptr) {
      int ret = ::munmap(ptr, length);
      TP_THROW_SYSTEM_IF(ret != 0, errno);
    }
  };

  std::unique_ptr<uint8_t[], Deleter> ptr_{nullptr, Deleter{}};
};

} // namespace tensorpipe
