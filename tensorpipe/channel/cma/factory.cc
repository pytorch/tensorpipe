/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/factory.h>

#include <tensorpipe/channel/cma/channel_impl.h>
#include <tensorpipe/channel/cma/context_impl.h>
#include <tensorpipe/channel/context_boilerplate.h>

namespace tensorpipe {
namespace channel {
namespace cma {

std::shared_ptr<Context> create() {
  return std::make_shared<ContextBoilerplate<ContextImpl, ChannelImpl>>();
}

} // namespace cma
} // namespace channel
} // namespace tensorpipe
