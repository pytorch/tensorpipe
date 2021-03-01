/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <thread>

#include <tensorpipe/common/error.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {

class CudaHostAllocatorClosedError final : public BaseError {
  std::string what() const override {
    return "CUDA host allocator closed";
  }
};

class CudaHostAllocator {
 private:
  class HostPtrDeleter {
   public:
    HostPtrDeleter(CudaHostAllocator& allocator);
    void operator()(uint8_t* ptr);

   private:
    CudaHostAllocator& allocator_;
  };

 public:
  using THostPtr = std::shared_ptr<uint8_t[]>;
  using TAllocCallback = std::function<void(const Error&, THostPtr)>;

  explicit CudaHostAllocator(
      size_t numChunks = 16,
      size_t chunkSize = 1024 * 1024);

  ~CudaHostAllocator();

  void alloc(size_t size, TAllocCallback callback);
  size_t getChunkLength() const;

  void close();
  void join();

 private:
  const size_t numChunks_;
  const size_t chunkSize_;
  const std::unique_ptr<uint8_t[], void (*)(uint8_t*)> data_;
  std::vector<bool> chunkAvailable_;
  size_t allocatedChunks_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<TAllocCallback> pendingAllocations_;
  bool closed_{false};
  std::atomic<bool> joined_{false};

  void processAllocations(std::unique_lock<std::mutex> lock);
  THostPtr getAvailableChunk();

  void hostPtrDeleter(uint8_t* ptr);
};

} // namespace tensorpipe
