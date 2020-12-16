/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/cuda_ipc/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/cuda_lib.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

class Context::PrivateIface {
 public:
  virtual ClosingEmitter& getClosingEmitter() = 0;

  virtual ~PrivateIface() = default;

  virtual CudaLib& getCudaLib() = 0;
};

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
