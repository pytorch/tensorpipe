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

CudaHostAllocator::CudaHostAllocator(size_t numChunks, size_t chunkSize)
    : numChunks_(numChunks),
      chunkSize_(chunkSize),
      data_(numChunks * chunkSize),
      chunkAvailable_(numChunks, true) {}

CudaHostAllocator::~CudaHostAllocator() {
  close();
  if (dataIsRegistered_) {
    TP_CUDA_CHECK(cudaHostUnregister(data_.data()));
  }
}

void CudaHostAllocator::alloc(size_t size, TAllocCallback callback) {
  TP_DCHECK(size <= chunkSize_);
  if (!dataIsRegistered_) {
    TP_CUDA_CHECK(
        cudaHostRegister(data_.data(), data_.size(), cudaHostRegisterDefault));
    dataIsRegistered_ = true;
  }
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
          data_.data() + curChunk * chunkSize_,
          [this](uint8_t* ptr) { releaseChunk(ptr); });
    }
  }

  return nullptr;
}

void CudaHostAllocator::releaseChunk(uint8_t* ptr) {
  size_t chunkId = (ptr - data_.data()) / chunkSize_;
  chunkAvailable_[chunkId] = true;
  --allocatedChunks_;
  processAllocations();
}

} // namespace tensorpipe
