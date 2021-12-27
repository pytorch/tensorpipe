/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_gdr/factory.h>

#include <tensorpipe/channel/context_boilerplate.h>
#include <tensorpipe/channel/cuda_gdr/channel_impl.h>
#include <tensorpipe/channel/cuda_gdr/context_impl.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

std::shared_ptr<Context> create(
    optional<std::vector<std::string>> gpuIdxToNicName) {
  return std::make_shared<ContextBoilerplate<ContextImpl, ChannelImpl>>(
      std::move(gpuIdxToNicName));
}

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
