/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/cuda_loop.h>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {

namespace {

struct CudaCallback {
  CudaLoop& loop;
  std::function<void(const Error&)> callback;

  CudaCallback(CudaLoop& loop, std::function<void(const Error&)> callback)
      : loop(loop), callback(std::move(callback)) {}
};

class CudaLoopClosedError final : public BaseError {
  std::string what() const override {
    return "CUDA loop already closed";
  }
};

} // namespace

CudaLoop::CudaLoop() {
  thread_ = std::thread([this]() {
    setThreadName("TP_CUDA_callback_loop");
    processCallbacks();
  });
}

CudaLoop::~CudaLoop() {
  join();
}

void CudaLoop::join() {
  close();

  if (!joined_.exchange(true)) {
    thread_.join();
  }
}

void CudaLoop::close() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }
  closed_ = true;
  cv_.notify_all();
}

void CudaLoop::processCallbacks() {
  for (;;) {
    std::deque<Operation> operations;
    {
      std::unique_lock<std::mutex> lock(mutex_);

      if (operations_.empty()) {
        if (closed_ && pendingOperations_ == 0) {
          break;
        } else {
          cv_.wait(lock);
        }
      }

      std::swap(operations, operations_);
      pendingOperations_ -= operations.size();
    }

    for (auto& op : operations) {
      op.callback(op.error);
    }
  }
}

void CudaLoop::addCallback(
    int device,
    cudaStream_t stream,
    std::function<void(const Error&)> callback) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) {
      callback(TP_CREATE_ERROR(CudaLoopClosedError));
      return;
    }
    ++pendingOperations_;
  }

  auto cudaCallback =
      std::make_unique<CudaCallback>(*this, std::move(callback));
  CudaDeviceGuard guard(device);
  TP_CUDA_CHECK(cudaStreamAddCallback(
      stream, runCudaCallback, cudaCallback.release(), 0));
}

void CUDART_CB CudaLoop::runCudaCallback(
    cudaStream_t /* unused */,
    cudaError_t cudaError,
    void* callbackPtr) {
  std::unique_ptr<CudaCallback> cudaCallback(
      reinterpret_cast<CudaCallback*>(callbackPtr));
  CudaLoop& loop = cudaCallback->loop;
  {
    std::unique_lock<std::mutex> lock(loop.mutex_);
    auto error = Error::kSuccess;
    if (cudaError != cudaSuccess) {
      error = TP_CREATE_ERROR(CudaError, cudaError);
    }
    loop.operations_.push_back(
        {std::move(cudaCallback->callback), std::move(error)});
    loop.cv_.notify_all();
  }
  cudaCallback.reset();
}

} // namespace tensorpipe
