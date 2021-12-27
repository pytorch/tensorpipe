/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

} // namespace

INSTANTIATE_TEST_CASE_P(Cma, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(Cma, CpuChannelTestSuite, ::testing::Values(&helper));
