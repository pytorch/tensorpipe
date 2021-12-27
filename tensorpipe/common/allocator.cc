/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/allocator.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {

Allocator::Allocator(uint8_t* data, size_t numChunks, size_t chunkSize)
    : numChunks_(numChunks),
      chunkSize_(chunkSize),
      data_(data),
      chunkAvailable_(numChunks, true) {}

Allocator::~Allocator() {
  close();
}

void Allocator::alloc(size_t size, TAllocCallback callback) {
  TP_DCHECK(size <= chunkSize_);
  pendingAllocations_.push_back(std::move(callback));
  processAllocations();
}

size_t Allocator::getChunkLength() const {
  return chunkSize_;
}

void Allocator::close() {
  if (closed_) {
    return;
  }
  closed_ = true;
  processAllocations();
}

void Allocator::processAllocations() {
  while (!pendingAllocations_.empty()) {
    auto& callback = pendingAllocations_.front();
    if (closed_) {
      callback(TP_CREATE_ERROR(AllocatorClosedError), nullptr);
    } else {
      TChunk ptr = getAvailableChunk();
      if (!ptr) {
        break;
      }
      callback(Error::kSuccess, std::move(ptr));
    }
    pendingAllocations_.pop_front();
  }
}

Allocator::TChunk Allocator::getAvailableChunk() {
  for (size_t curChunk = 0; curChunk < numChunks_; ++curChunk) {
    if (chunkAvailable_[curChunk]) {
      chunkAvailable_[curChunk] = false;
      ++allocatedChunks_;
      return TChunk(data_ + curChunk * chunkSize_, [this](uint8_t* ptr) {
        releaseChunk(ptr);
      });
    }
  }

  return nullptr;
}

void Allocator::releaseChunk(uint8_t* ptr) {
  size_t chunkId = (ptr - data_) / chunkSize_;
  chunkAvailable_[chunkId] = true;
  --allocatedChunks_;
  processAllocations();
}

} // namespace tensorpipe
