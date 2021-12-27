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

#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/test/channel/channel_test.h>

class CpuDataWrapper : public DataWrapper {
 public:
  explicit CpuDataWrapper(size_t length) : vector_(length) {}

  explicit CpuDataWrapper(std::vector<uint8_t> v) : vector_(v) {}

  tensorpipe::Buffer buffer() const override {
    return tensorpipe::CpuBuffer{.ptr = const_cast<uint8_t*>(vector_.data())};
  }

  size_t bufferLength() const override {
    return vector_.size();
  }

  std::vector<uint8_t> unwrap() override {
    return vector_;
  }

 private:
  std::vector<uint8_t> vector_;
};

class CpuChannelTestHelper : public ChannelTestHelper {
 public:
  std::unique_ptr<DataWrapper> makeDataWrapper(size_t length) override {
    return std::make_unique<CpuDataWrapper>(length);
  }

  std::unique_ptr<DataWrapper> makeDataWrapper(
      std::vector<uint8_t> v) override {
    return std::make_unique<CpuDataWrapper>(std::move(v));
  }
};

class CpuChannelTestSuite
    : public ::testing::TestWithParam<CpuChannelTestHelper*> {};
