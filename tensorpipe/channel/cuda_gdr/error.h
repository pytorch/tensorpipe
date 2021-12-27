/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <tensorpipe/channel/error.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

class IbvError final : public BaseError {
 public:
  explicit IbvError(std::string error) : error_(error) {}

  std::string what() const override {
    return error_;
  }

 private:
  std::string error_;
};

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
