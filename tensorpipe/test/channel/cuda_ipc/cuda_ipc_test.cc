/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/cuda_ipc/factory.h>
#include <tensorpipe/test/channel/channel_test_cuda.h>

namespace {

class CudaIpcChannelTestHelper : public CudaChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto context = tensorpipe::channel::cuda_ipc::create();
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ProcessPeerGroup>();
  }
};

CudaIpcChannelTestHelper helper;

class CudaIpcChannelTestSuite : public ChannelTestSuite {};

} // namespace

class CannotCommunicateInSameProcessTest : public ChannelTestCase {
 public:
  void run(ChannelTestHelper* /* unused */) override {
    ForkedThreadPeerGroup pg;
    pg.spawn(
        [&]() {
          auto ctx = tensorpipe::channel::cuda_ipc::create();
          auto deviceDescriptors = ctx->deviceDescriptors();
          EXPECT_GT(deviceDescriptors.size(), 0);
          auto descriptor = deviceDescriptors.begin()->second;
          // From within a given process, the device descriptors will be the
          // same.
          EXPECT_FALSE(ctx->canCommunicateWithRemote(descriptor, descriptor));
        },
        [&]() {
          // Do nothing.
        });
  }
};

CHANNEL_TEST(CudaIpcChannelTestSuite, CannotCommunicateInSameProcess);

INSTANTIATE_TEST_CASE_P(CudaIpc, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaIpc,
    CudaChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaIpc,
    CudaMultiGPUChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaIpc,
    CudaIpcChannelTestSuite,
    ::testing::Values(&helper));
