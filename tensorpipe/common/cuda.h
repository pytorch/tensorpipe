/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <iomanip>
#include <ios>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <cuda_runtime.h>

#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/strings.h>

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

  explicit CudaEvent(int device, bool interprocess = false)
      : deviceIdx_(device) {
    CudaDeviceGuard guard(deviceIdx_);
    int flags = cudaEventDisableTiming;
    if (interprocess) {
      flags |= cudaEventInterprocess;
    }
    TP_CUDA_CHECK(cudaEventCreateWithFlags(&ev_, flags));
  }

  explicit CudaEvent(int device, cudaIpcEventHandle_t handle)
      : deviceIdx_(device) {
    // It could crash if we don't set device when creating events from handles
    CudaDeviceGuard guard(deviceIdx_);
    TP_CUDA_CHECK(cudaIpcOpenEventHandle(&ev_, handle));
  }

  void record(cudaStream_t stream) {
    CudaDeviceGuard guard(deviceIdx_);
    TP_CUDA_CHECK(cudaEventRecord(ev_, stream));
  }

  void wait(cudaStream_t stream, int device) {
    CudaDeviceGuard guard(device);
    TP_CUDA_CHECK(cudaStreamWaitEvent(stream, ev_, 0));
  }

  bool query() const {
    CudaDeviceGuard guard(deviceIdx_);
    cudaError_t res = cudaEventQuery(ev_);
    if (res == cudaErrorNotReady) {
      return false;
    }
    TP_CUDA_CHECK(res);
    return true;
  }

  cudaEvent_t raw() {
    return ev_;
  }

  cudaIpcEventHandle_t getIpcHandle() const {
    CudaDeviceGuard guard(deviceIdx_);
    cudaIpcEventHandle_t handle;
    TP_CUDA_CHECK(cudaIpcGetEventHandle(&handle, ev_));
    return handle;
  }

  std::string serializedHandle() {
    cudaIpcEventHandle_t handle = getIpcHandle();
    return std::string(reinterpret_cast<const char*>(&handle), sizeof(handle));
  }

  ~CudaEvent() {
    CudaDeviceGuard guard(deviceIdx_);
    TP_CUDA_CHECK(cudaEventDestroy(ev_));
  }

 private:
  cudaEvent_t ev_;
  int deviceIdx_;
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

  int deviceIdx;
  TP_CUDA_DRIVER_CHECK(
      cudaLib,
      cudaLib.pointerGetAttribute(
          &deviceIdx,
          CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
          reinterpret_cast<CUdeviceptr>(ptr)));

  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.ctxSetCurrent(ctx));
  return deviceIdx;
}

class CudaPinnedMemoryDeleter {
 public:
  explicit CudaPinnedMemoryDeleter(int deviceIdx) : deviceIdx_(deviceIdx) {}

  void operator()(uint8_t* ptr) {
    CudaDeviceGuard guard(deviceIdx_);
    TP_CUDA_CHECK(cudaFreeHost(ptr));
  }

 private:
  const int deviceIdx_;
};

using CudaPinnedBuffer = std::unique_ptr<uint8_t[], CudaPinnedMemoryDeleter>;

inline CudaPinnedBuffer makeCudaPinnedBuffer(size_t length, int deviceIdx) {
  CudaDeviceGuard guard(deviceIdx);
  uint8_t* ptr;
  TP_CUDA_CHECK(cudaMallocHost(&ptr, length));
  return CudaPinnedBuffer(ptr, CudaPinnedMemoryDeleter(deviceIdx));
}

class CudaDeviceBuffer {
 public:
  CudaDeviceBuffer() = default;

  CudaDeviceBuffer(size_t length, int deviceIdx) {
    CudaDeviceGuard guard(deviceIdx);
    uint8_t* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, length));
    ptr_ = {ptr, Deleter{deviceIdx}};
  }

  uint8_t* ptr() const {
    return ptr_.get();
  }

  int deviceIdx() const {
    return ptr_.get_deleter().deviceIdx;
  }

  void reset() {
    ptr_.reset();
  }

  cudaIpcMemHandle_t getIpcHandle() const {
    CudaDeviceGuard guard(deviceIdx());
    cudaIpcMemHandle_t handle;
    TP_CUDA_CHECK(cudaIpcGetMemHandle(&handle, ptr_.get()));
    return handle;
  }

 private:
  struct Deleter {
    int deviceIdx;

    void operator()(uint8_t* ptr) {
      CudaDeviceGuard guard(deviceIdx);
      TP_CUDA_CHECK(cudaFree(ptr));
    }
  };

  std::unique_ptr<uint8_t[], Deleter> ptr_;
};

class CudaIpcBuffer {
 public:
  CudaIpcBuffer() = default;

  CudaIpcBuffer(int deviceIdx, const cudaIpcMemHandle_t& handle) {
    CudaDeviceGuard guard(deviceIdx);
    void* ptr;
    TP_CUDA_CHECK(
        cudaIpcOpenMemHandle(&ptr, handle, cudaIpcMemLazyEnablePeerAccess));
    ptr_ = {reinterpret_cast<uint8_t*>(ptr), Deleter{deviceIdx}};
  }

  uint8_t* ptr() const {
    return ptr_.get();
  }

  int deviceIdx() const {
    return ptr_.get_deleter().deviceIdx;
  }

  void reset() {
    ptr_.reset();
  }

 private:
  struct Deleter {
    int deviceIdx;

    void operator()(uint8_t* ptr) {
      CudaDeviceGuard guard(deviceIdx);
      TP_CUDA_CHECK(cudaIpcCloseMemHandle(ptr));
    }
  };

  std::unique_ptr<uint8_t[], Deleter> ptr_;
};

inline std::string getUuidOfDevice(const CudaLib& cudaLib, int deviceIdx) {
  CUdevice device;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.deviceGet(&device, deviceIdx));

  CUuuid uuid;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.deviceGetUuid(&uuid, device));

  // The CUDA driver and NVML choose two different format for UUIDs, hence we
  // need to reconcile them. We do so using the most human readable format, that
  // is "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" (8-4-4-4-12).
  std::ostringstream uuidSs;
  uuidSs << std::hex << std::setfill('0');
  for (int j = 0; j < 16; ++j) {
    // The bitmask is required otherwise a negative value will get promoted to
    // (signed) int with sign extension if char is signed.
    uuidSs << std::setw(2) << (uuid.bytes[j] & 0xff);
    if (j == 3 || j == 5 || j == 7 || j == 9) {
      uuidSs << '-';
    }
  }

  std::string uuidStr = uuidSs.str();
  TP_THROW_ASSERT_IF(!isValidUuid(uuidStr))
      << "Couldn't obtain valid UUID for GPU #" << deviceIdx
      << " from CUDA driver. Got: " << uuidStr;

  return uuidStr;
}

inline std::vector<std::string> getUuidsOfVisibleDevices(
    const CudaLib& cudaLib) {
  int deviceCount;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.deviceGetCount(&deviceCount));

  std::vector<std::string> result(deviceCount);
  for (int devIdx = 0; devIdx < deviceCount; ++devIdx) {
    result[devIdx] = getUuidOfDevice(cudaLib, devIdx);
  }

  return result;
}

inline std::vector<Device> getCudaDevices(const CudaLib& cudaLib) {
  int deviceCount;
  TP_CUDA_DRIVER_CHECK(cudaLib, cudaLib.deviceGetCount(&deviceCount));
  std::vector<Device> result(deviceCount);
  for (int devIdx = 0; devIdx < deviceCount; ++devIdx) {
    result[devIdx] = Device{kCudaDeviceType, devIdx};
  }

  return result;
}

} // namespace tensorpipe
