/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cuda_runtime.h>

// This kernel takes time and puts garbage data in the buffer. It is used to
// test proper synchronization in CUDA channels.
void slowKernel(void* ptr, int kSize, cudaStream_t stream);
