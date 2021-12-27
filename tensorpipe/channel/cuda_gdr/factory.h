/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <vector>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

std::shared_ptr<Context> create(
    optional<std::vector<std::string>> gpuIdxToNicName = nullopt);

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
