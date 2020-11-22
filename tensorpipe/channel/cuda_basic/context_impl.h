/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/cuda_basic/context.h>
#include <tensorpipe/common/callback.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

class Context::PrivateIface {
 public:
  virtual ClosingEmitter& getClosingEmitter() = 0;

  virtual ~PrivateIface() = default;
};

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
