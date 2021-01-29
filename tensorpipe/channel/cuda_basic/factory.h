/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/channel/cpu_context.h>
#include <tensorpipe/channel/cuda_context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

std::shared_ptr<CudaContext> create(std::shared_ptr<CpuContext> cpuContext);

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
