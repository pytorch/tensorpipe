/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {
namespace shm {

std::shared_ptr<Context> create();

} // namespace shm
} // namespace transport
} // namespace tensorpipe
