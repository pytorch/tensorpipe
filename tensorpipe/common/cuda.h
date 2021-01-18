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

#include <tensorpipe/common/cuda_lib.h>
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

class CudaDeviceGuard {
 public:
  CudaDeviceGuard() = delete;
  CudaDeviceGuard(const CudaDeviceGuard&) = delete;
  CudaDeviceGuard(CudaDeviceGuard&&) = delete;
  CudaDeviceGuard& operator=(const CudaDeviceGuard&) = delete;
  CudaDeviceGuard& operator=(CudaDeviceGuard&&) = delete;

  explicit CudaDeviceGuard(int device) {
    TP_CUDA_CHECK(cudaGetDevice(&device_));
    TP_CUDA_CHECK(cudaSetDevice(device));
  }

  ~CudaDeviceGuard() {
    TP_CUDA_CHECK(cudaSetDevice(device_));
  }

 private:
  int device_;
};

class CudaEvent {
 public:
  CudaEvent() = delete;
  CudaEvent(const CudaEvent&) = delete;
  CudaEvent(CudaEvent&&) = delete;
  CudaEvent& operator=(const CudaEvent&) = delete;
  CudaEvent& operator=(CudaEvent&&) = delete;

  explicit CudaEvent(int device, bool interprocess = false) {
    CudaDeviceGuard guard(device);
    int flags = cudaEventDisableTiming;
    if (interprocess) {
      flags |= cudaEventInterprocess;
    }
    TP_CUDA_CHECK(cudaEventCreateWithFlags(&ev_, flags));
  }

  explicit CudaEvent(int device, cudaIpcEventHandle_t handle) {
    // It could crash if we don't set device when creating events from handles
    CudaDeviceGuard guard(device);
    TP_CUDA_CHECK(cudaIpcOpenEventHandle(&ev_, handle));
  }

  void record(cudaStream_t stream) {
    TP_CUDA_CHECK(cudaEventRecord(ev_, stream));
  }

  void wait(cudaStream_t stream, int device) {
    CudaDeviceGuard guard(device);
    TP_CUDA_CHECK(cudaStreamWaitEvent(stream, ev_, 0));
  }

  bool query() const {
    cudaError_t res = cudaEventQuery(ev_);
    if (res == cudaErrorNotReady) {
      return false;
    }
    TP_CUDA_CHECK(res);
    return true;
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

inline int cudaDeviceForPointer(const CudaLib& cudaLib, const void* ptr) {
  // When calling cudaSetDevice(0) when device 0 hasn't been initialized yet
  // the CUDA runtime sets the current context of the CUDA driver to what's
  // apparently an invalid non-null value. This causes cudaPointerGetAttributes
  // to misbehave (possibly other functions too, but this is the only function
  // that we call outside of a device guard). In fact, device guards are likely
  // the reason we call cudaSetDevice(0) at all, because at destruction they
  // reset the current device to the value it had before construction, and that
  // will be zero if no other device guard was active at that point.
  // The ugly workaround is to manually undo the runtime's errors, by clearing
  // the driver's current context. In a sense, by creating a "reverse" guard.
  CUcontext ctx;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.ctxGetCurrent(&ctx));
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.ctxSetCurrent(nullptr));

  cudaPointerAttributes attrs;
  TP_CUDA_CHECK(cudaPointerGetAttributes(&attrs, ptr));
#if (CUDART_VERSION >= 10000)
  TP_DCHECK_EQ(cudaMemoryTypeDevice, attrs.type);
#else
  TP_DCHECK_EQ(cudaMemoryTypeDevice, attrs.memoryType);
#endif

  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.ctxSetCurrent(ctx));
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
