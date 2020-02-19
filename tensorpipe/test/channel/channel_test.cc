/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/channel_test.h>

#include <numeric>

#include <tensorpipe/common/queue.h>

using namespace tensorpipe;
using namespace tensorpipe::channel;

TEST_P(ChannelTest, Name) {
  std::shared_ptr<ChannelFactory> factory = GetParam()->makeFactory();
  EXPECT_EQ(factory->name(), GetParam()->getName());
}

TEST_P(ChannelTest, DomainDescriptor) {
  std::shared_ptr<ChannelFactory> factory1 = GetParam()->makeFactory();
  std::shared_ptr<ChannelFactory> factory2 = GetParam()->makeFactory();
  EXPECT_FALSE(factory1->domainDescriptor().empty());
  EXPECT_FALSE(factory2->domainDescriptor().empty());
  EXPECT_EQ(factory1->domainDescriptor(), factory2->domainDescriptor());
}

TEST_P(ChannelTest, CreateChannel) {
  std::shared_ptr<ChannelFactory> factory1 = GetParam()->makeFactory();
  std::shared_ptr<ChannelFactory> factory2 = GetParam()->makeFactory();
  constexpr auto dataSize = 256;
  Queue<Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory1->createChannel(
            std::move(conn), Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Perform send and wait for completion.
        Channel::TDescriptor descriptor;
        std::future<Error> future;
        std::tie(descriptor, future) =
            sendWithFuture(channel, data.data(), data.size());
        descriptorQueue.push(std::move(descriptor));
        ASSERT_FALSE(future.get());
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory2->createChannel(
            std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with zeroes.
        std::vector<uint8_t> data(dataSize, 0);

        // Perform recv and wait for completion.
        std::future<Error> future = recvWithFuture(
            channel, descriptorQueue.pop(), data.data(), data.size());
        ASSERT_FALSE(future.get());

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }
      });

  factory1->join();
  factory2->join();
}
