/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sycl/sycl.hpp>
#include <tensorpipe/common/device.h>

namespace tensorpipe {

struct XpuBuffer {
  void* ptr{nullptr};
  sycl::queue* queue{nullptr};

  Device getDevice() const;
};
} // namespace tensorpipe
