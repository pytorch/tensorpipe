/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include <sycl/sycl.hpp>
#include <tensorpipe/common/xpu.h>
#include <tensorpipe/common/xpu_buffer.h>
#include <tensorpipe/test/channel/channel_test.h>

class XpuDataWrapper : public DataWrapper {
 public:
  XpuDataWrapper(const XpuDataWrapper&) = delete;
  XpuDataWrapper& operator=(const XpuDataWrapper&) = delete;

  explicit XpuDataWrapper(size_t length) : length_(length) {
    if (length_ > 0) {
      queue_ = &tensorpipe::xpu::getDefaultXPUQueue(0);
      ptr_ = sycl::malloc_device<uint8_t>(length_, *queue_);
    }
  }

  explicit XpuDataWrapper(std::vector<uint8_t> v) : XpuDataWrapper(v.size()) {
    if (length_ > 0) {
      queue_->memcpy(ptr_, v.data(), length_).wait();
    }
  }

  tensorpipe::Buffer buffer() const override {
    return tensorpipe::XpuBuffer{
        .ptr = ptr_,
        .queue = queue_,
    };
  }

  size_t bufferLength() const override {
    return length_;
  }

  std::vector<uint8_t> unwrap() override {
    std::vector<uint8_t> v(length_);
    if (length_ > 0) {
      queue_->memcpy(v.data(), ptr_, length_).wait();
    }
    return v;
  }

  ~XpuDataWrapper() override {
    if (ptr_ != nullptr) {
      sycl::free(ptr_, *queue_);
    }
  }

 private:
  uint8_t* ptr_{nullptr};
  size_t length_{0};
  sycl::queue* queue_{nullptr};
};

class XpuChannelTestHelper : public ChannelTestHelper {
 public:
  std::unique_ptr<DataWrapper> makeDataWrapper(size_t length) override {
    return std::make_unique<XpuDataWrapper>(length);
  }

  std::unique_ptr<DataWrapper> makeDataWrapper(
      std::vector<uint8_t> v) override {
    return std::make_unique<XpuDataWrapper>(std::move(v));
  }
};

class XpuChannelTestSuite
    : public ::testing::TestWithParam<XpuChannelTestHelper*> {};

class XpuMultiGPUChannelTestSuite
    : public ::testing::TestWithParam<XpuChannelTestHelper*> {};
