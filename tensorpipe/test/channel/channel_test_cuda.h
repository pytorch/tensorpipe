/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/test/channel/channel_test.h>

class CudaDataWrapper : public DataWrapper {
 public:
  // Non-copyable.
  CudaDataWrapper(const CudaDataWrapper&) = delete;
  CudaDataWrapper& operator=(const CudaDataWrapper&) = delete;
  // Non-movable.
  CudaDataWrapper(CudaDataWrapper&& other) = delete;
  CudaDataWrapper& operator=(CudaDataWrapper&& other) = delete;

  explicit CudaDataWrapper(size_t length) : length_(length) {
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaSetDevice(0));
      TP_CUDA_CHECK(cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking));
      TP_CUDA_CHECK(cudaMalloc(&cudaPtr_, length_));
    }
  }

  explicit CudaDataWrapper(std::vector<uint8_t> v) : CudaDataWrapper(v.size()) {
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaMemcpyAsync(
          cudaPtr_, v.data(), length_, cudaMemcpyDefault, stream_));
    }
  }

  tensorpipe::Buffer buffer() const override {
    return tensorpipe::CudaBuffer{
        .ptr = cudaPtr_,
        .stream = stream_,
    };
  }

  size_t bufferLength() const override {
    return length_;
  }

  std::vector<uint8_t> unwrap() override {
    std::vector<uint8_t> v(length_);
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaStreamSynchronize(stream_));
      TP_CUDA_CHECK(cudaMemcpy(v.data(), cudaPtr_, length_, cudaMemcpyDefault));
    }
    return v;
  }

  ~CudaDataWrapper() override {
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaFree(cudaPtr_));
      TP_CUDA_CHECK(cudaStreamDestroy(stream_));
    }
  }

 private:
  void* cudaPtr_{nullptr};
  size_t length_{0};
  cudaStream_t stream_{cudaStreamDefault};
};

class CudaChannelTestHelper : public ChannelTestHelper {
 public:
  std::unique_ptr<DataWrapper> makeDataWrapper(size_t length) override {
    return std::make_unique<CudaDataWrapper>(length);
  }

  std::unique_ptr<DataWrapper> makeDataWrapper(
      std::vector<uint8_t> v) override {
    return std::make_unique<CudaDataWrapper>(std::move(v));
  }
};

class CudaChannelTestSuite
    : public ::testing::TestWithParam<CudaChannelTestHelper*> {};

class CudaMultiGPUChannelTestSuite
    : public ::testing::TestWithParam<CudaChannelTestHelper*> {};

class CudaXDTTChannelTestSuite
    : public ::testing::TestWithParam<CudaChannelTestHelper*> {};
