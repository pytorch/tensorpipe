/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/mpt/factory.h>

#include <tensorpipe/channel/mpt/context.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

std::shared_ptr<CpuContext> create(
    std::vector<std::shared_ptr<transport::Context>> contexts,
    std::vector<std::shared_ptr<transport::Listener>> listeners) {
  return std::make_shared<mpt::Context>(
      std::move(contexts), std::move(listeners));
}

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
