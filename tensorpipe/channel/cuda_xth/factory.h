/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/channel/cuda_context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_xth {

std::shared_ptr<CudaContext> create();

} // namespace cuda_xth
} // namespace channel
} // namespace tensorpipe
