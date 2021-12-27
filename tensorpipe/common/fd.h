/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include <type_traits>

#include <unistd.h>

#include <tensorpipe/common/error.h>

namespace tensorpipe {

class Fd {
 public:
  Fd() = default;

  explicit Fd(int fd) : fd_(fd) {}

  virtual ~Fd() {
    reset();
  }

  // Disable copy constructor.
  Fd(const Fd&) = delete;

  // Disable copy assignment.
  Fd& operator=(const Fd&) = delete;

  // Custom move constructor.
  Fd(Fd&& other) noexcept {
    std::swap(fd_, other.fd_);
  }

  // Custom move assignment.
  Fd& operator=(Fd&& other) noexcept {
    std::swap(fd_, other.fd_);
    return *this;
  }

  // Return underlying file descriptor.
  int fd() const {
    return fd_;
  }

  bool hasValue() const {
    return fd_ >= 0;
  }

  void reset() {
    if (hasValue()) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  // Proxy to read(2) with EINTR retry.
  ssize_t read(void* buf, size_t count);

  // Proxy to write(2) with EINTR retry.
  ssize_t write(const void* buf, size_t count);

  // Call read and return error if it doesn't exactly read `count` bytes.
  Error readFull(void* buf, size_t count);

  // Call write and return error if it doesn't exactly write `count` bytes.
  Error writeFull(const void* buf, size_t count);

  // Call `readFull` with trivially copyable type. Throws on errors.
  template <typename T>
  T readOrThrow() {
    T tmp;
    static_assert(std::is_trivially_copyable<T>::value, "!");
    auto err = readFull(&tmp, sizeof(T));
    if (err) {
      throw std::runtime_error(err.what());
    }
    return tmp;
  }

  // Call `writeFull` with trivially copyable type. Throws on errors.
  template <typename T>
  void writeOrThrow(const T& t) {
    static_assert(std::is_trivially_copyable<T>::value, "!");
    auto err = writeFull(&t, sizeof(T));
    if (err) {
      throw std::runtime_error(err.what());
    }
  }

  // Call `readFull` with trivially copyable type.
  template <typename T>
  Error read(T* t) {
    static_assert(std::is_trivially_copyable<T>::value, "!");
    return readFull(t, sizeof(T));
  }

  // Call `writeFull` with trivially copyable type.
  template <typename T>
  Error write(const T& t) {
    static_assert(std::is_trivially_copyable<T>::value, "!");
    return writeFull(&t, sizeof(T));
  }

 protected:
  int fd_{-1};
};

} // namespace tensorpipe
