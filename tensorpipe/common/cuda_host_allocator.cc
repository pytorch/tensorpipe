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

uint8_t* allocPinnedBuffer(size_t size) {
  uint8_t* ptr;
  TP_CUDA_CHECK(cudaMallocHost(&ptr, size));
  return ptr;
}

void freePinnedBuffer(uint8_t* ptr) {
  TP_CUDA_CHECK(cudaFreeHost(ptr));
}

} // namespace

CudaHostAllocator::CudaHostAllocator(size_t numChunks, size_t chunkSize)
    : numChunks_(numChunks),
      chunkSize_(chunkSize),
      data_(allocPinnedBuffer(numChunks * chunkSize), freePinnedBuffer),
      chunkAvailable_(numChunks, true) {}

CudaHostAllocator::~CudaHostAllocator() {
  join();
}

void CudaHostAllocator::alloc(size_t size, TAllocCallback callback) {
  std::unique_lock<std::mutex> lock(mutex_);
  TP_DCHECK(size <= chunkSize_);
  pendingAllocations_.push_back(std::move(callback));
  processAllocations(std::move(lock));
}

size_t CudaHostAllocator::getChunkLength() const {
  return chunkSize_;
}

void CudaHostAllocator::close() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }
  closed_ = true;
  processAllocations(std::move(lock));
}

void CudaHostAllocator::join() {
  close();
  if (!joined_.exchange(true)) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this]() {
      return pendingAllocations_.empty() && (allocatedChunks_ == 0);
    });
  }
}

void CudaHostAllocator::processAllocations(std::unique_lock<std::mutex> lock) {
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
          [this](uint8_t* ptr) { hostPtrDeleter(ptr); });
    }
  }

  return nullptr;
}

void CudaHostAllocator::hostPtrDeleter(uint8_t* ptr) {
  size_t chunkId = (ptr - data_.get()) / chunkSize_;
  std::unique_lock<std::mutex> lock(mutex_);
  chunkAvailable_[chunkId] = true;
  --allocatedChunks_;
  processAllocations(std::move(lock));
  cv_.notify_all();
}

} // namespace tensorpipe
