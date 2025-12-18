/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <sycl/sycl.hpp>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace tensorpipe {
class XpuLoop {
  struct Op {
    int deviceIdx;
    sycl::event event;
    std::function<void(const Error&)> fn;
    Error error;
  };

 public:
  XpuLoop();
  ~XpuLoop();

  void addCallback(
      int deviceIdx,
      sycl::event event,
      std::function<void(const Error&)> fn);

  void close();
  void join();

 private:
  void run();
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<Op> pending_;
  bool done_{false};
};
} // namespace tensorpipe
