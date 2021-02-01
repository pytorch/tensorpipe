/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_ipc/factory.h>

#include <tensorpipe/channel/cuda_ipc/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

std::shared_ptr<CudaContext> create() {
  return std::make_shared<cuda_ipc::Context>();
}

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
