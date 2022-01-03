/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_gdr_xdtt/factory.h>

#include <tensorpipe/channel/context_boilerplate.h>
#include <tensorpipe/channel/cuda_gdr_xdtt/channel_impl.h>
#include <tensorpipe/channel/cuda_gdr_xdtt/context_impl.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr_xdtt {

std::shared_ptr<Context> create(
    optional<std::vector<std::string>> gpuIdxToNicName) {
  return std::make_shared<ContextBoilerplate<ContextImpl, ChannelImpl>>(
      std::move(gpuIdxToNicName));
}

} // namespace cuda_gdr_xdtt
} // namespace channel
} // namespace tensorpipe
