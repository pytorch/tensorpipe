/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

class AllocatorClosedError final : public BaseError {
  std::string what() const override {
    return "allocator closed";
  }
};

class Allocator {
 public:
  // Note: this is a std::shared_ptr<uint8_t[]> semantically. A shared_ptr with
  // array type is supported in C++17 and higher.
  using TChunk = std::shared_ptr<uint8_t>;
  using TAllocCallback = std::function<void(const Error&, TChunk)>;

  explicit Allocator(uint8_t* data, size_t numChunks, size_t chunkSize);

  ~Allocator();

  void alloc(size_t size, TAllocCallback callback);
  size_t getChunkLength() const;

  void close();

 private:
  const size_t numChunks_;
  const size_t chunkSize_;
  uint8_t* const data_;
  std::vector<bool> chunkAvailable_;
  size_t allocatedChunks_{0};
  std::deque<TAllocCallback> pendingAllocations_;
  bool closed_{false};

  void processAllocations();
  TChunk getAvailableChunk();
  void releaseChunk(uint8_t* ptr);
};

} // namespace tensorpipe
