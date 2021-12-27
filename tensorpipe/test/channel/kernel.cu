/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cuda.h>

__global__ void _slowKernel(char* ptr, int sz) {
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  for (; idx < sz; idx += (gridDim.x * blockDim.x)) {
    for (int i = 0; i < 100000; ++i) {
      ptr[idx] += ptr[(idx + 1007) % sz] + i;
    }
  }
}

void slowKernel(void* ptr, int kSize, cudaStream_t stream) {
  _slowKernel<<<128, 128, 0, stream>>>((char*)ptr, kSize);
}
