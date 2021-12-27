/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

class IbvError final : public BaseError {
 public:
  explicit IbvError(std::string error) : error_(error) {}

  std::string what() const override;

 private:
  std::string error_;
};

class GetaddrinfoError final : public BaseError {
 public:
  explicit GetaddrinfoError(int error) : error_(error) {}

  std::string what() const override;

 private:
  int error_;
};

class NoAddrFoundError final : public BaseError {
 public:
  NoAddrFoundError() {}

  std::string what() const override;
};

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
