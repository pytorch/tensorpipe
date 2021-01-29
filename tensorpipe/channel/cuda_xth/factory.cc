/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_xth/factory.h>

#include <tensorpipe/channel/cuda_xth/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_xth {

std::shared_ptr<CudaContext> create() {
  return std::make_shared<cuda_xth::Context>();
}

} // namespace cuda_xth
} // namespace channel
} // namespace tensorpipe
