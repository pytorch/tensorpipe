/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/cuda_gdr/factory.h>
#include <tensorpipe/test/channel/channel_test_cuda.h>

namespace {

class CudaGdrChannelTestHelper : public CudaChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto context = tensorpipe::channel::cuda_gdr::create();
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ProcessPeerGroup>();
  }
};

CudaGdrChannelTestHelper helper;

class CudaGdrChannelTestSuite : public ChannelTestSuite {};

} // namespace

class CudaGdrCanCommunicateWithRemoteTest
    : public CanCommunicateWithRemoteTest {
 public:
  void checkDeviceDescriptors(
      const tensorpipe::channel::Context& ctx,
      const std::unordered_map<tensorpipe::Device, std::string>&
          localDeviceDescriptors,
      const std::unordered_map<tensorpipe::Device, std::string>&
          remoteDeviceDescriptors) const override {
    for (const auto& localIt : localDeviceDescriptors) {
      for (const auto& remoteIt : remoteDeviceDescriptors) {
        EXPECT_TRUE(
            ctx.canCommunicateWithRemote(localIt.second, remoteIt.second));
      }
    }
  }
};

CHANNEL_TEST(CudaGdrChannelTestSuite, CudaGdrCanCommunicateWithRemote);

INSTANTIATE_TEST_CASE_P(CudaGdr, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaGdr,
    CudaChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaGdr,
    CudaMultiGPUChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaGdr,
    CudaGdrChannelTestSuite,
    ::testing::Values(&helper));
