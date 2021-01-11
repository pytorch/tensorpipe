/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <mutex>
#include <vector>

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

class CudaPinnedBufferAllocator {
 public:
  CudaPinnedBufferAllocator(const CudaPinnedBufferAllocator&) = delete;
  CudaPinnedBufferAllocator(CudaPinnedBufferAllocator&&) = delete;
  CudaPinnedBufferAllocator& operator=(const CudaPinnedBufferAllocator&) =
      delete;
  CudaPinnedBufferAllocator& operator=(CudaPinnedBufferAllocator&&) = delete;

  CudaPinnedBufferAllocator(size_t numChunks = 1024, size_t chunkSize = 1024)
      : kNumChunks_(numChunks),
        kChunkSize_(chunkSize),
        chunkAvailable_(numChunks, false),
        basePtr_(allocPinnedBuffer(numChunks * chunkSize)) {}

  std::shared_ptr<uint8_t> getBuffer(size_t size) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      size_t requestedChunks = (size + kChunkSize_ - 1) / kChunkSize_;
      size_t startChunk = 0;

      // Linear check for available contiguous chunks.
      for (size_t curChunk = 0; curChunk < kNumChunks_; ++curChunk) {
        if (!chunkAvailable_[curChunk]) {
          startChunk = curChunk + 1;
        } else if (curChunk - startChunk + 1 == requestedChunks) {
          for (size_t i = startChunk; i <= curChunk; ++i) {
            chunkAvailable_[i] = false;
          }

          return std::shared_ptr<uint8_t>(
              basePtr_.get() + startChunk * kChunkSize_,
              [this, startChunk, curChunk](uint8_t* /* unused */) {
                for (size_t i = startChunk; i <= curChunk; ++i) {
                  // There is a harmless race condition here.
                  chunkAvailable_[i] = true;
                }
              });
        }
      }
    }

    // No contiguous pre-allocated chunks available, falling back to allocating
    // a temporary buffer.
    return allocPinnedBuffer(size);
  }

 private:
  const size_t kNumChunks_;
  const size_t kChunkSize_;
  std::mutex mutex_;
  std::vector<bool> chunkAvailable_;
  std::shared_ptr<uint8_t> basePtr_;

  std::shared_ptr<uint8_t> allocPinnedBuffer(size_t size) {
    void* ptr;
    TP_CUDA_CHECK(cudaMallocHost(&ptr, size));
    return std::shared_ptr<uint8_t>(
        reinterpret_cast<uint8_t*>(ptr),
        [](uint8_t* ptr) { TP_CUDA_CHECK(cudaFreeHost(ptr)); });
  }
};

} // namespace tensorpipe
