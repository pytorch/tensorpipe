/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_ipc/factory.h>

#include <tensorpipe/channel/context_boilerplate.h>
#include <tensorpipe/channel/cuda_ipc/channel_impl.h>
#include <tensorpipe/channel/cuda_ipc/context_impl.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

std::shared_ptr<Context> create() {
  return std::make_shared<ContextBoilerplate<ContextImpl, ChannelImpl>>();
}

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
