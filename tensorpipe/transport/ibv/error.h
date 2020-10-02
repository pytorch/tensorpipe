/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/ibv/ibv.h>

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

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
