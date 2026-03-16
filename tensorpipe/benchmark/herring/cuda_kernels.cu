/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>

#include <cuda.h>

// Copied from PyTorch's aten/src/ATen/native/cuda/Loops.cuh

constexpr size_t warp_size = 32;
constexpr size_t num_threads = warp_size * 2;
constexpr size_t thread_work_size = 4;
constexpr size_t block_work_size = thread_work_size * num_threads;

#define CUDA_CHECK(op)                        \
  {                                           \
    cudaError_t res = (op);                   \
    if (res != cudaSuccess) {                 \
      throw std::runtime_error("CUDA error"); \
    }                                         \
  }

namespace {

template <typename T>
T ceilOfRatio(T num, T den) {
  return (num - 1) / den + 1;
}

__global__ void atomicAddIntoKernel(float* dst, float* src, size_t len) {
  for (size_t idx = blockIdx.x * blockDim.x + threadIdx.x; idx < len;
       idx += (gridDim.x * blockDim.x)) {
    atomicAdd(dst + idx, *(src + idx));
  }
}

} // namespace

void atomicAddInto(float* dst, float* src, size_t len, cudaStream_t stream) {
  int64_t grid = ceilOfRatio(len, block_work_size);
  atomicAddIntoKernel<<<grid, num_threads, 0, stream>>>(dst, src, len);
  CUDA_CHECK(cudaGetLastError());
}
