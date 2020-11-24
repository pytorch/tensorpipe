/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/cuda_loop.h>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {

CudaLoop::CudaLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  thread_ = std::thread([this, lock{std::move(lock)}]() mutable {
    setThreadName("TP_CUDA_callback_loop");
    for (;;) {
      if (operations_.empty()) {
        if (joined_ && pendingOperations_ == 0) {
          break;
        } else {
          cv_.wait(lock);
        }
      }

      std::deque<Operation> operations;
      std::swap(operations, operations_);

      lock.unlock();
      for (auto& op : operations) {
        op.callback(op.error);
      }
      lock.lock();

      pendingOperations_ -= operations.size();
    }
  });
}

CudaLoop::~CudaLoop() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    joined_ = true;
    cv_.notify_all();
  }
  thread_.join();
}

void CudaLoop::addCallback(cudaStream_t stream, TCudaCallback callback) {
  std::unique_lock<std::mutex> lock(mutex_);
  TP_THROW_ASSERT_IF(joined_);
  ++pendingOperations_;
  auto it = cudaCallbacks_.emplace(cudaCallbacks_.end());
  *it =
      [this, it, callback{std::move(callback)}](cudaError_t cudaError) mutable {
        std::unique_lock<std::mutex> lock(mutex_);
        TP_THROW_ASSERT_IF(joined_);
        operations_.push_back({std::move(callback), cudaError});
        cv_.notify_all();
        // This destroys the lambda, which is fine as long as no captured
        // variable is being used passed that point.
        cudaCallbacks_.erase(it);
      };
  TP_CUDA_CHECK(cudaStreamAddCallback(stream, runCudaCallback, &*it, 0));
}

void CUDART_CB CudaLoop::runCudaCallback(
    cudaStream_t /* unused */,
    cudaError_t error,
    void* callbackPtr) {
  auto& callback = *reinterpret_cast<TCudaCallback*>(callbackPtr);
  callback(error);
}

} // namespace tensorpipe
