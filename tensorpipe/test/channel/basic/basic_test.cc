/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/basic.h>
#include <tensorpipe/test/channel/channel_test.h>

using namespace tensorpipe;

TEST(BasicChannelFactoryTest, Name) {
  std::shared_ptr<channel::ChannelFactory> factory =
      std::make_shared<channel::basic::BasicChannelFactory>();
  EXPECT_EQ(factory->name(), "basic");
}

INSTANTIATE_TYPED_TEST_CASE_P(
    Basic,
    ChannelFactoryTest,
    channel::basic::BasicChannelFactory);
