/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// Note: never include this file from headers!

#include <string>

#include <tensorpipe/common/nop.h>

namespace tensorpipe {
namespace channel {

std::string saveDescriptor(const AbstractNopHolder& object);

void loadDescriptor(AbstractNopHolder& object, const std::string& in);

} // namespace channel
} // namespace tensorpipe
