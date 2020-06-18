/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <tensorpipe/common/error.h>

namespace tensorpipe {
namespace channel {

class SystemError final : public BaseError {
 public:
  explicit SystemError(const char* syscall, int error)
      : syscall_(syscall), error_(error) {}

  std::string what() const override;

 private:
  const char* syscall_;
  const int error_;
};

class ShortReadError final : public BaseError {
 public:
  ShortReadError(ssize_t expected, ssize_t actual)
      : expected_(expected), actual_(actual) {}

  std::string what() const override;

 private:
  const ssize_t expected_;
  const ssize_t actual_;
};

class ShortWriteError final : public BaseError {
 public:
  ShortWriteError(ssize_t expected, ssize_t actual)
      : expected_(expected), actual_(actual) {}

  std::string what() const override;

 private:
  const ssize_t expected_;
  const ssize_t actual_;
};

class EOFError final : public BaseError {
 public:
  EOFError() {}

  std::string what() const override;
};

class ContextClosedError final : public BaseError {
 public:
  ContextClosedError() {}

  std::string what() const override;
};

class ChannelClosedError final : public BaseError {
 public:
  ChannelClosedError() {}

  std::string what() const override;
};

} // namespace channel
} // namespace tensorpipe
