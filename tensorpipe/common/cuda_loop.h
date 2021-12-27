/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <mutex>
#include <thread>

#include <cuda_runtime.h>

#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {

class CudaLoop {
  struct Operation {
    std::function<void(const Error&)> callback;
    Error error;
  };

 public:
  CudaLoop();

  ~CudaLoop();

  void join();
  void close();

  void addCallback(
      int device,
      cudaStream_t stream,
      std::function<void(const Error&)> callback);

 private:
  std::thread thread_;
  std::deque<Operation> operations_;
  std::mutex mutex_;
  std::condition_variable cv_;
  uint64_t pendingOperations_{0};

  bool closed_{false};
  std::atomic<bool> joined_{false};

  void processCallbacks();

  // Proxy static method for cudaStreamAddCallback(), which does not accept
  // lambdas.
  static void CUDART_CB runCudaCallback(
      cudaStream_t stream,
      cudaError_t cudaError,
      void* callbackPtr);
};

} // namespace tensorpipe
