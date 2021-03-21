/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/context.h>

namespace tensorpipe {
namespace channel {

// FIXME: These aliases are temporarily required for backwards compatibility.
// Remove once dependent code has been updated.
using CpuChannel = Channel;
using CpuContext = Context;

} // namespace channel
} // namespace tensorpipe
