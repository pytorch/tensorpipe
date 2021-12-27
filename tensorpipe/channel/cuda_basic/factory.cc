/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_basic/factory.h>

#include <tensorpipe/channel/context_boilerplate.h>
#include <tensorpipe/channel/cuda_basic/channel_impl.h>
#include <tensorpipe/channel/cuda_basic/context_impl.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

std::shared_ptr<Context> create(std::shared_ptr<Context> cpuContext) {
  return std::make_shared<ContextBoilerplate<ContextImpl, ChannelImpl>>(
      std::move(cpuContext));
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
