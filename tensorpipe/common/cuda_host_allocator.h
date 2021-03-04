/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <vector>

#include <tensorpipe/common/error.h>

namespace tensorpipe {

class CudaHostAllocatorClosedError final : public BaseError {
  std::string what() const override {
    return "CUDA host allocator closed";
  }
};

class CudaPinnedMemoryDeleter {
 public:
  CudaPinnedMemoryDeleter(int deviceIdx);
  void operator()(uint8_t* ptr);

 private:
  const int deviceIdx_;
};

class CudaHostAllocator {
 public:
  using THostPtr = std::shared_ptr<uint8_t[]>;
  using TAllocCallback = std::function<void(const Error&, THostPtr)>;

  explicit CudaHostAllocator(
      int deviceIdx,
      size_t numChunks = 16,
      size_t chunkSize = 1024 * 1024);

  ~CudaHostAllocator();

  void alloc(size_t size, TAllocCallback callback);
  size_t getChunkLength() const;

  void close();

 private:
  const size_t numChunks_;
  const size_t chunkSize_;
  const std::unique_ptr<uint8_t[], CudaPinnedMemoryDeleter> data_;
  std::vector<bool> chunkAvailable_;
  size_t allocatedChunks_{0};
  std::deque<TAllocCallback> pendingAllocations_;
  bool closed_{false};

  void processAllocations();
  THostPtr getAvailableChunk();
  void releaseChunk(uint8_t* ptr);
};

} // namespace tensorpipe
