/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/xpu_buffer.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/xpu.h>

namespace tensorpipe {

Device XpuBuffer::getDevice() const {
  return Device{kXpuDeviceType, xpu::xpuDeviceForPointer(ptr)};
}

} // namespace tensorpipe
