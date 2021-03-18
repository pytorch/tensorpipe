/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/config.h>

#include <tensorpipe/common/cpu_buffer.h>
#if TENSORPIPE_SUPPORTS_CUDA
#include <tensorpipe/common/cuda_buffer.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

namespace tensorpipe {

enum class DeviceType {
  kCpu,
#if TENSORPIPE_SUPPORTS_CUDA
  kCuda,
#endif // TENSORPIPE_SUPPORTS_CUDA
};

struct Buffer {
  Buffer() {}

  /* implicit */ Buffer(CpuBuffer buffer)
      : type(DeviceType::kCpu), cpu(buffer) {}

  Buffer& operator=(CpuBuffer& buffer) {
    type = DeviceType::kCpu;
    cpu = buffer;

    return *this;
  }

#if TENSORPIPE_SUPPORTS_CUDA
  /* implicit */ Buffer(CudaBuffer buffer)
      : type(DeviceType::kCuda), cuda(buffer) {}

  Buffer& operator=(CudaBuffer& buffer) {
    type = DeviceType::kCuda;
    cuda = buffer;

    return *this;
  }

#endif // TENSORPIPE_SUPPORTS_CUDA

  DeviceType type{DeviceType::kCpu};
  union {
    CpuBuffer cpu;
#if TENSORPIPE_SUPPORTS_CUDA
    CudaBuffer cuda;
#endif // TENSORPIPE_SUPPORTS_CUDA
  };
};

} // namespace tensorpipe
