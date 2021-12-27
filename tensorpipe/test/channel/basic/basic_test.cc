/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/factory.h>
#include <tensorpipe/test/channel/channel_test_cpu.h>

namespace {

class BasicChannelTestHelper : public CpuChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto context = tensorpipe::channel::basic::create();
    context->setId(std::move(id));
    return context;
  }
};

BasicChannelTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(Basic, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(Basic, CpuChannelTestSuite, ::testing::Values(&helper));
