/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/xth/factory.h>

#include <tensorpipe/channel/xth/context.h>

namespace tensorpipe {
namespace channel {
namespace xth {

std::shared_ptr<CpuContext> create() {
  return std::make_shared<xth::Context>();
}

} // namespace xth
} // namespace channel
} // namespace tensorpipe
