/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cuda_runtime.h>

#include <tensorpipe/common/device.h>

namespace tensorpipe {

struct CudaBuffer {
  void* ptr{nullptr};
  cudaStream_t stream{cudaStreamDefault};

  Device getDevice() const;
};

} // namespace tensorpipe
