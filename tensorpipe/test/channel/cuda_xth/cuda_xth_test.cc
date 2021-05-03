/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/cuda_xth/factory.h>
#include <tensorpipe/test/channel/channel_test_cuda.h>

namespace {

class CudaXthChannelTestHelper : public CudaChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto context = tensorpipe::channel::cuda_xth::create();
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ForkedThreadPeerGroup>();
  }
};

CudaXthChannelTestHelper helper;

class CudaXthChannelTestSuite : public ChannelTestSuite {};

} // namespace

class CudaXthCanCommunicateWithRemoteTest
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
        if (localIt.first.type == tensorpipe::kCpuDeviceType &&
            remoteIt.first.type == tensorpipe::kCpuDeviceType) {
          EXPECT_FALSE(
              ctx.canCommunicateWithRemote(localIt.second, remoteIt.second));
        } else {
          EXPECT_TRUE(
              ctx.canCommunicateWithRemote(localIt.second, remoteIt.second));
        }
      }
    }
  }
};

CHANNEL_TEST(CudaXthChannelTestSuite, CudaXthCanCommunicateWithRemote);

INSTANTIATE_TEST_CASE_P(CudaXth, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaXth,
    CudaChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaXth,
    CudaMultiGPUChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaXth,
    CudaXDTTChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaXth,
    CudaXthChannelTestSuite,
    ::testing::Values(&helper));
