/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <mutex>
#include <thread>

#include <cuda_runtime.h>

namespace tensorpipe {

class CudaLoop {
  using TCudaCallback = std::function<void(cudaError_t)>;

  struct Operation {
    TCudaCallback callback;
    cudaError_t error;
  };

 public:
  CudaLoop();

  ~CudaLoop();

  // TODO: device parameter?
  void addCallback(cudaStream_t stream, TCudaCallback callback);

 private:
  std::thread thread_;
  std::list<TCudaCallback> cudaCallbacks_;
  std::deque<Operation> operations_;
  std::mutex mutex_;
  std::condition_variable cv_;
  uint64_t pendingOperations_ = 0;
  bool joined_ = false;

  // Proxy static method for cudaStreamAddCallback(), which does not accept
  // lambdas.
  static void CUDART_CB runCudaCallback(
      cudaStream_t /* unused */,
      cudaError_t error,
      void* callbackPtr);
};

} // namespace tensorpipe
