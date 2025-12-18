/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/basic/factory.h>
#include <tensorpipe/channel/xpu_basic/factory.h>
#include <tensorpipe/test/channel/channel_test_xpu.h>

namespace {

class XpuBasicChannelTestHelper : public XpuChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto cpuContext = tensorpipe::channel::basic::create();
    auto context =
        tensorpipe::channel::xpu_basic::create(std::move(cpuContext));
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ProcessPeerGroup>();
  }
};

XpuBasicChannelTestHelper helper;

class XpuBasicChannelTestSuite : public ChannelTestSuite {};

} // namespace


INSTANTIATE_TEST_CASE_P(XpuBasic, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    XpuBasic,
    XpuChannelTestSuite,
    ::testing::Values(&helper));
