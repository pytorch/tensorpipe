/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>

namespace tensorpipe {

struct CpuBuffer {
  void* ptr{nullptr};
  size_t length{0};
};

} // namespace tensorpipe
