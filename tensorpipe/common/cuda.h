/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <cuda_runtime.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>

#define TP_CUDA_CHECK(a)                                                \
  do {                                                                  \
    cudaError_t error = (a);                                            \
    TP_THROW_ASSERT_IF(cudaSuccess != error)                            \
        << __TP_EXPAND_OPD(a) << " " << cudaGetErrorName(error) << " (" \
        << cudaGetErrorString(error) << ")";                            \
  } while (false)

namespace tensorpipe {

class CudaError final : public BaseError {
 public:
  explicit CudaError(cudaError_t error) : error_(error) {}

  std::string what() const override {
    return std::string(cudaGetErrorString(error_));
  }

 private:
  cudaError_t error_;
};

class CudaEvent {
 public:
  explicit CudaEvent(int device, bool interprocess = false) {
    TP_CUDA_CHECK(cudaSetDevice(device));
    int flags = cudaEventDisableTiming;
    if (interprocess) {
      flags |= cudaEventInterprocess;
    }
    TP_CUDA_CHECK(cudaEventCreateWithFlags(&ev_, flags));
  }

  explicit CudaEvent(cudaIpcEventHandle_t handle) {
    TP_CUDA_CHECK(cudaIpcOpenEventHandle(&ev_, handle));
  }

  void record(cudaStream_t stream) {
    TP_CUDA_CHECK(cudaEventRecord(ev_, stream));
  }

  void wait(cudaStream_t stream, int device) {
    TP_CUDA_CHECK(cudaSetDevice(device));
    TP_CUDA_CHECK(cudaStreamWaitEvent(stream, ev_, 0));
  }

  std::string serializedHandle() {
    cudaIpcEventHandle_t handle;
    TP_CUDA_CHECK(cudaIpcGetEventHandle(&handle, ev_));

    return std::string(reinterpret_cast<const char*>(&handle), sizeof(handle));
  }

  ~CudaEvent() {
    TP_CUDA_CHECK(cudaEventDestroy(ev_));
  }

 private:
  cudaEvent_t ev_;
};

inline int cudaDeviceForPointer(const void* ptr) {
  cudaPointerAttributes attrs;
  TP_CUDA_CHECK(cudaPointerGetAttributes(&attrs, ptr));
#if (CUDART_VERSION >= 10000)
  TP_DCHECK_EQ(cudaMemoryTypeDevice, attrs.type);
#else
  TP_DCHECK_EQ(cudaMemoryTypeDevice, attrs.memoryType);
#endif
  return attrs.device;
}

using CudaPinnedBuffer = std::shared_ptr<uint8_t>;

inline CudaPinnedBuffer makeCudaPinnedBuffer(size_t length) {
  void* ptr;
  TP_CUDA_CHECK(cudaMallocHost(&ptr, length));
  return CudaPinnedBuffer(reinterpret_cast<uint8_t*>(ptr), [](uint8_t* ptr) {
    TP_CUDA_CHECK(cudaFreeHost(ptr));
  });
}

} // namespace tensorpipe
