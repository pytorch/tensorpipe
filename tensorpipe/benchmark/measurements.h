/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <chrono>
#include <vector>

namespace tensorpipe {
namespace benchmark {

class Measurements {
  using clock = std::chrono::high_resolution_clock;
  using nanoseconds = std::chrono::nanoseconds;

 public:
  void markStart() {
    start_ = clock::now();
  }

  void markStop(size_t count = 1) {
    samples_.push_back((clock::now() - start_) / count);
  }

  void sort() {
    std::sort(samples_.begin(), samples_.end());
  }

  void reserve(size_t capacity) {
    samples_.reserve(capacity);
  }

  size_t size() const {
    return samples_.size();
  }

  nanoseconds sum() const {
    nanoseconds sum{0};
    for (const auto& sample : samples_) {
      sum += sample;
    }
    return sum;
  }

  nanoseconds percentile(float f) const {
    return samples_[static_cast<size_t>(f * samples_.size())];
  }

 private:
  clock::time_point start_;
  std::vector<nanoseconds> samples_;
};

} // namespace benchmark
} // namespace tensorpipe
