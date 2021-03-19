/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>

#include <tensorpipe/common/buffer.h>

namespace tensorpipe {

struct CpuBuffer {
  void* ptr{nullptr};
  size_t length{0};

  DeviceType deviceType() const {
    return DeviceType::kCpu;
  }
};

} // namespace tensorpipe
