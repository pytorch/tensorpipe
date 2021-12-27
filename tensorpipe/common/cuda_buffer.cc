/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/cuda_buffer.h>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>

namespace tensorpipe {

Device CudaBuffer::getDevice() const {
  static CudaLib cudaLib = []() {
    Error error;
    CudaLib lib;
    std::tie(error, lib) = CudaLib::create();
    TP_THROW_ASSERT_IF(error)
        << "Cannot get CUDA device for pointer because libcuda could not be loaded: "
        << error.what();
    return lib;
  }();

  return Device{kCudaDeviceType, cudaDeviceForPointer(cudaLib, ptr)};
}

} // namespace tensorpipe
