/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/cpu_buffer.h>

namespace tensorpipe {
namespace channel {

using CpuChannel = Channel<CpuBuffer>;
using CpuContext = Context<CpuBuffer>;

} // namespace channel
} // namespace tensorpipe
