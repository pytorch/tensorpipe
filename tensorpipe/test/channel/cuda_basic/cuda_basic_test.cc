/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/basic/factory.h>
#include <tensorpipe/channel/cuda_basic/factory.h>
#include <tensorpipe/test/channel/channel_test_cuda.h>

namespace {

class CudaBasicChannelTestHelper : public CudaChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto cpuContext = tensorpipe::channel::basic::create();
    auto context =
        tensorpipe::channel::cuda_basic::create(std::move(cpuContext));
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ProcessPeerGroup>();
  }
};

CudaBasicChannelTestHelper helper;

class CudaBasicChannelTestSuite : public ChannelTestSuite {};

} // namespace

class CannotCommunicateCpuToCpuTest : public ChannelTestCase {
 public:
  void run(ChannelTestHelper* /* unused */) override {
    ForkedThreadPeerGroup pg;
    pg.spawn(
        [&]() {
          auto cpuContext = tensorpipe::channel::basic::create();
          auto ctx =
              tensorpipe::channel::cuda_basic::create(std::move(cpuContext));
          auto deviceDescriptors = ctx->deviceDescriptors();
          auto it = deviceDescriptors.find(
              tensorpipe::Device{tensorpipe::kCpuDeviceType, 0});
          EXPECT_FALSE(it == deviceDescriptors.end());
          auto descriptor = it->second;
          EXPECT_FALSE(ctx->canCommunicateWithRemote(descriptor, descriptor));
        },
        [&]() {
          // Do nothing.
        });
  }
};

CHANNEL_TEST(CudaBasicChannelTestSuite, CannotCommunicateCpuToCpu);

INSTANTIATE_TEST_CASE_P(
    CudaBasic,
    ChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaBasic,
    CudaChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaBasic,
    CudaMultiGPUChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaBasic,
    CudaXDTTChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaBasic,
    CudaBasicChannelTestSuite,
    ::testing::Values(&helper));
