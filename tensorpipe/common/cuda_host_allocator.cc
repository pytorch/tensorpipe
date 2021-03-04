/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/cuda_host_allocator.h>

#include <cuda_runtime.h>

#include <tensorpipe/common/cuda.h>

namespace tensorpipe {

namespace {

uint8_t* allocPinnedBuffer(int deviceIdx, size_t size) {
  CudaDeviceGuard guard(deviceIdx);
  uint8_t* ptr;
  TP_CUDA_CHECK(cudaMallocHost(&ptr, size));
  return ptr;
}

void freePinnedBuffer(int deviceIdx, uint8_t* ptr) {
  CudaDeviceGuard guard(deviceIdx);
  TP_CUDA_CHECK(cudaFreeHost(ptr));
}

} // namespace

CudaHostAllocator::CudaHostAllocator(
    int deviceIdx,
    size_t numChunks,
    size_t chunkSize)
    : numChunks_(numChunks),
      chunkSize_(chunkSize),
      data_(
          allocPinnedBuffer(deviceIdx, numChunks * chunkSize),
          [deviceIdx](uint8_t* ptr) { freePinnedBuffer(deviceIdx, ptr); }),
      chunkAvailable_(numChunks, true) {}

CudaHostAllocator::~CudaHostAllocator() {
  close();
}

void CudaHostAllocator::alloc(size_t size, TAllocCallback callback) {
  TP_DCHECK(size <= chunkSize_);
  pendingAllocations_.push_back(std::move(callback));
  processAllocations();
}

size_t CudaHostAllocator::getChunkLength() const {
  return chunkSize_;
}

void CudaHostAllocator::close() {
  if (closed_) {
    return;
  }
  closed_ = true;
  processAllocations();
}

void CudaHostAllocator::processAllocations() {
  while (!pendingAllocations_.empty()) {
    auto& callback = pendingAllocations_.front();
    if (closed_) {
      callback(TP_CREATE_ERROR(CudaHostAllocatorClosedError), nullptr);
    } else {
      THostPtr ptr = getAvailableChunk();
      if (!ptr) {
        break;
      }
      callback(Error::kSuccess, std::move(ptr));
    }
    pendingAllocations_.pop_front();
  }
}

CudaHostAllocator::THostPtr CudaHostAllocator::getAvailableChunk() {
  for (size_t curChunk = 0; curChunk < numChunks_; ++curChunk) {
    if (chunkAvailable_[curChunk]) {
      chunkAvailable_[curChunk] = false;
      ++allocatedChunks_;
      return THostPtr(
          data_.get() + curChunk * chunkSize_,
          [this](uint8_t* ptr) { releaseChunk(ptr); });
    }
  }

  return nullptr;
}

void CudaHostAllocator::releaseChunk(uint8_t* ptr) {
  size_t chunkId = (ptr - data_.get()) / chunkSize_;
  chunkAvailable_[chunkId] = true;
  --allocatedChunks_;
  processAllocations();
}

} // namespace tensorpipe
