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
  factory->join();
}

TEST_P(ChannelTest, DomainDescriptor) {
  std::shared_ptr<ChannelFactory> factory1 = GetParam()->makeFactory();
  std::shared_ptr<ChannelFactory> factory2 = GetParam()->makeFactory();
  EXPECT_FALSE(factory1->domainDescriptor().empty());
  EXPECT_FALSE(factory2->domainDescriptor().empty());
  EXPECT_EQ(factory1->domainDescriptor(), factory2->domainDescriptor());
}

TEST_P(ChannelTest, ClientToServer) {
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
        std::vector<uint8_t> data(dataSize);
        std::fill(data.begin(), data.end(), 0);

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

TEST_P(ChannelTest, ServerToClient) {
  std::shared_ptr<ChannelFactory> factory1 = GetParam()->makeFactory();
  std::shared_ptr<ChannelFactory> factory2 = GetParam()->makeFactory();
  constexpr auto dataSize = 256;
  Queue<Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory1->createChannel(
            std::move(conn), Channel::Endpoint::kListen);

        // Initialize with zeroes.
        std::vector<uint8_t> data(dataSize);
        std::fill(data.begin(), data.end(), 0);

        // Perform recv and wait for completion.
        std::future<Error> future = recvWithFuture(
            channel, descriptorQueue.pop(), data.data(), data.size());
        ASSERT_FALSE(future.get());

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory2->createChannel(
            std::move(conn), Channel::Endpoint::kConnect);

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
      });

  factory1->join();
  factory2->join();
}

TEST_P(ChannelTest, FactoryIsNotJoined) {
  std::shared_ptr<ChannelFactory> factory = GetParam()->makeFactory();

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        factory->createChannel(std::move(conn), Channel::Endpoint::kListen);
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        factory->createChannel(std::move(conn), Channel::Endpoint::kConnect);
      });
}

TEST_P(ChannelTest, CallbacksAreDeferred) {
  // This test wants to make sure that the "heavy lifting" of copying data isn't
  // performed inline inside the recv method as that would make the user-facing
  // read method of the pipe blocking. However, since we can't really check that
  // behavior, we'll check a highly correlated one: that the recv callback isn't
  // called inline from within the recv method. We do so by having that behavior
  // cause a deadlock.
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
        std::promise<Error> promise;
        std::mutex mutex;
        std::unique_lock<std::mutex> callerLock(mutex);
        Channel::TDescriptor descriptor = channel->send(
            data.data(), data.size(), [&promise, &mutex](const Error& error) {
              std::unique_lock<std::mutex> calleeLock(mutex);
              promise.set_value(error);
            });
        callerLock.unlock();
        descriptorQueue.push(std::move(descriptor));
        ASSERT_FALSE(promise.get_future().get());
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory2->createChannel(
            std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with zeroes.
        std::vector<uint8_t> data(dataSize);
        std::fill(data.begin(), data.end(), 0);

        // Perform recv and wait for completion.
        std::promise<Error> promise;
        std::mutex mutex;
        std::unique_lock<std::mutex> callerLock(mutex);
        channel->recv(
            descriptorQueue.pop(),
            data.data(),
            data.size(),
            [&promise, &mutex](const Error& error) {
              std::unique_lock<std::mutex> calleeLock(mutex);
              promise.set_value(error);
            });
        callerLock.unlock();
        ASSERT_FALSE(promise.get_future().get());

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }
      });

  factory1->join();
  factory2->join();
}
