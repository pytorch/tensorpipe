/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/ibv.h>
#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

class IbvError final : public BaseError {
 public:
  explicit IbvError(enum ibv_wc_status status) : status_(status) {}

  std::string what() const override;

 private:
  enum ibv_wc_status status_;
};

class GetaddrinfoError final : public BaseError {
 public:
  GetaddrinfoError(int error) : error_(error) {}

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
