/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/config.h>

#if TENSORPIPE_HAS_CUDA
#include <cuda_runtime.h>
#endif // TENSORPIPE_HAS_CUDA

namespace tensorpipe {

enum class DeviceType {
  kCpu,
#if TENSORPIPE_HAS_CUDA
  kCuda,
#endif // TENSORPIPE_HAS_CUDA
};

struct CpuBuffer {
  void* ptr{nullptr};
  size_t length{0};
};

#if TENSORPIPE_HAS_CUDA
struct CudaBuffer {
  void* ptr{nullptr};
  size_t length{0};
  cudaStream_t stream{cudaStreamDefault};
};
#endif // TENSORPIPE_HAS_CUDA

struct Buffer {
  Buffer(CpuBuffer t) : type(DeviceType::kCpu), cpu(t) {}

#if TENSORPIPE_HAS_CUDA
  Buffer(CudaBuffer t) : type(DeviceType::kCuda), cuda(t) {}
#endif // TENSORPIPE_HAS_CUDA

  DeviceType type;
  union {
    CpuBuffer cpu;
#if TENSORPIPE_HAS_CUDA
    CudaBuffer cuda;
#endif // TENSORPIPE_HAS_CUDA
  };
};

} // namespace tensorpipe
