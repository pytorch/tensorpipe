/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// Note: never include this file from headers!

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/nop.h>

namespace tensorpipe {
namespace channel {

TDescriptor saveDescriptor(const AbstractNopHolder& object);

void loadDescriptor(AbstractNopHolder& object, const TDescriptor& in);

} // namespace channel
} // namespace tensorpipe
