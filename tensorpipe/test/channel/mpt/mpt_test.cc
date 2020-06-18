/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/mpt/context.h>
#include <tensorpipe/test/channel/channel_test.h>

namespace {

class MptChannelTestHelper : public ChannelTestHelper {
 public:
  std::shared_ptr<tensorpipe::channel::Context> makeContext(
      std::string id) override {
    std::vector<std::shared_ptr<tensorpipe::transport::Context>> contexts = {
        std::make_shared<tensorpipe::transport::uv::Context>(),
        std::make_shared<tensorpipe::transport::uv::Context>(),
        std::make_shared<tensorpipe::transport::uv::Context>()};
    std::vector<std::shared_ptr<tensorpipe::transport::Listener>> listeners = {
        contexts[0]->listen("127.0.0.1"),
        contexts[1]->listen("127.0.0.1"),
        contexts[2]->listen("127.0.0.1")};
    auto context = std::make_shared<tensorpipe::channel::mpt::Context>(
        std::move(contexts), std::move(listeners));
    context->setId(std::move(id));
    return context;
  }
};

MptChannelTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(Mpt, ChannelTest, ::testing::Values(&helper));
