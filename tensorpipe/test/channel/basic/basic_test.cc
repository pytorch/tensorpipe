/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/context.h>
#include <tensorpipe/test/channel/channel_test.h>

namespace {

class BasicChannelTestHelper : public ChannelTestHelper {
 public:
  std::shared_ptr<tensorpipe::channel::Context> makeContext(
      std::string id) override {
    auto context = std::make_shared<tensorpipe::channel::basic::Context>();
    context->setId(std::move(id));
    return context;
  }
};

BasicChannelTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(Basic, ChannelTest, ::testing::Values(&helper));
