/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/channel/context.h>

namespace tensorpipe {
namespace channel {
namespace xpu_basic {

std::shared_ptr<Context> create(std::shared_ptr<Context> cpuContext);

} // namespace xpu_basic
} // namespace channel
} // namespace tensorpipe
