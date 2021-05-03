/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/factory.h>
#include <tensorpipe/test/channel/channel_test_cpu.h>

namespace {

class CmaChannelTestHelper : public CpuChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto context = tensorpipe::channel::cma::create();
    context->setId(std::move(id));
    return context;
  }
};

CmaChannelTestHelper helper;

class CmaChannelTestSuite : public ChannelTestSuite {};

} // namespace

class CmaCanCommunicateWithRemoteTest : public CanCommunicateWithRemoteTest {
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

CHANNEL_TEST(CmaChannelTestSuite, CmaCanCommunicateWithRemote);

INSTANTIATE_TEST_CASE_P(Cma, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(Cma, CpuChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(Cma, CmaChannelTestSuite, ::testing::Values(&helper));
