/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_basic/factory.h>

#include <tensorpipe/channel/cuda_basic/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

std::shared_ptr<CudaContext> create(std::shared_ptr<CpuContext> cpuContext) {
  return std::make_shared<cuda_basic::Context>(std::move(cpuContext));
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
