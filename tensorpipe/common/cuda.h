/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cuda_runtime.h>

#include <tensorpipe/common/defs.h>

#define TP_CUDA_CHECK(a)                                                      \
  TP_THROW_ASSERT_IF(cudaSuccess != (a))                                      \
      << __TP_EXPAND_OPD(a) << " " << cudaGetErrorName(cudaPeekAtLastError()) \
      << " (" << cudaGetErrorString(cudaPeekAtLastError()) << ")"

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
