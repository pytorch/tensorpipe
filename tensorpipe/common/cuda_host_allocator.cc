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

uint8_t* CudaHostAllocator::allocPinnedBuffer(size_t size) {
  uint8_t* ptr;
  TP_CUDA_CHECK(cudaMallocHost(&ptr, size));
  return ptr;
}

void CudaHostAllocator::freePinnedBuffer(uint8_t* ptr) {
  TP_CUDA_CHECK(cudaFreeHost(ptr));
}

} // namespace

CudaHostAllocator::CudaHostAllocator(size_t numChunks, size_t chunkSize)
    : numChunks_(numChunks),
      chunkSize_(chunkSize),
      data_(allocPinnedBuffer(numChunks * chunkSize), freePinnedBuffer),
      chunkAvailable_(numChunks, true) {
  std::unique_lock<std::mutex> lock(mutex_);
  thread_ = std::thread([this, lock{std::move(lock)}]() mutable {
    setThreadName("TP_CUDA_host_allocator_loop");
    allocLoop(std::move(lock));
  });
}

CudaHostAllocator::~CudaHostAllocator() {
  join();
}

void CudaHostAllocator::alloc(size_t size, TAllocCallback callback) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (closed_) {
    callback(TP_CREATE_ERROR(CudaHostAllocatorClosedError), THostPtr());
    return;
  }
  TP_DCHECK(size <= chunkSize_);
  pendingAllocations_.push_back(std::move(callback));
  cv_.notify_all();
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
  cv_.notify_all();
}

void CudaHostAllocator::join() {
  close();
  if (!joined_.exchange(true)) {
    thread_.join();
  }
}

void CudaHostAllocator::allocLoop(std::unique_lock<std::mutex> lock) {
  for (;;) {
    if (closed_ && pendingAllocations_.empty()) {
      break;
    } else {
      cv_.wait(lock);
    }

    std::deque<TAllocCallback> allocations;
    std::swap(allocations, pendingAllocations_);
    lock.unlock();
    processAllocations(allocations);
    lock.lock();
    pendingAllocations_.insert(
        pendingAllocations_.begin(), allocations.begin(), allocations.end());
  }
}

void CudaHostAllocator::processAllocations(
    std::deque<TAllocCallback>& allocations) {
  for (size_t curChunk = 0; curChunk < numChunks_; ++curChunk) {
    if (allocations.empty()) {
      return;
    }
    if (chunkAvailable_[curChunk]) {
      chunkAvailable_[curChunk] = false;

      // FIXME: Ensure object remains alive until all chunks have been
      // reclaimed.
      THostPtr ptr(data_.get() + curChunk * chunkSize_, [this](uint8_t* ptr) {
        hostPtrDeleter(ptr);
      });
      auto& callback = allocations.front();
      callback(Error::kSuccess, std::move(ptr));

      allocations.pop_front();
    }
  }
}

void CudaHostAllocator::hostPtrDeleter(uint8_t* ptr) {
  size_t chunkId = (ptr - data_.get()) / chunkSize_;
  std::unique_lock<std::mutex> lock(mutex_);
  chunkAvailable_[chunkId] = true;
  cv_.notify_all();
}

} // namespace tensorpipe
