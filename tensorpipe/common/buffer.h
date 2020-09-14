/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/config.h>

#if TENSORPIPE_SUPPORTS_CUDA
#include <cuda_runtime.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

namespace tensorpipe {

enum class DeviceType {
  kCpu,
#if TENSORPIPE_SUPPORTS_CUDA
  kCuda,
#endif // TENSORPIPE_SUPPORTS_CUDA
};

struct CpuBuffer {
  void* ptr{nullptr};
  size_t length{0};
};

#if TENSORPIPE_SUPPORTS_CUDA
struct CudaBuffer {
  void* ptr{nullptr};
  size_t length{0};
  cudaStream_t stream{cudaStreamDefault};
};
#endif // TENSORPIPE_SUPPORTS_CUDA

struct Buffer {
  /* implicit */ Buffer(CpuBuffer t) : type(DeviceType::kCpu), cpu(t) {}

#if TENSORPIPE_SUPPORTS_CUDA
  /* implicit */ Buffer(CudaBuffer t) : type(DeviceType::kCuda), cuda(t) {}
#endif // TENSORPIPE_SUPPORTS_CUDA

  DeviceType type;
  union {
    CpuBuffer cpu;
#if TENSORPIPE_SUPPORTS_CUDA
    CudaBuffer cuda;
#endif // TENSORPIPE_SUPPORTS_CUDA
  };
};

} // namespace tensorpipe
