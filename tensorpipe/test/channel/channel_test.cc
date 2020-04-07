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
  std::shared_ptr<Context> context = GetParam()->makeContext();
  EXPECT_EQ(context->name(), GetParam()->getName());
  context->join();
}

TEST_P(ChannelTest, DomainDescriptor) {
  std::shared_ptr<Context> context1 = GetParam()->makeContext();
  std::shared_ptr<Context> context2 = GetParam()->makeContext();
  EXPECT_FALSE(context1->domainDescriptor().empty());
  EXPECT_FALSE(context2->domainDescriptor().empty());
  EXPECT_EQ(context1->domainDescriptor(), context2->domainDescriptor());
}

TEST_P(ChannelTest, ClientToServer) {
  std::shared_ptr<Context> context1 = GetParam()->makeContext();
  std::shared_ptr<Context> context2 = GetParam()->makeContext();
  constexpr auto dataSize = 256;
  Queue<Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = context1->createChannel(
            std::move(conn), Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Perform send and wait for completion.
        std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
        std::future<Error> future;
        std::tie(descriptorFuture, future) =
            sendWithFuture(channel, data.data(), data.size());
        Error error;
        Channel::TDescriptor descriptor;
        std::tie(error, descriptor) = descriptorFuture.get();
        ASSERT_FALSE(error);
        descriptorQueue.push(std::move(descriptor));
        ASSERT_FALSE(future.get());
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = context2->createChannel(
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

  context1->join();
  context2->join();
}

TEST_P(ChannelTest, ServerToClient) {
  std::shared_ptr<Context> context1 = GetParam()->makeContext();
  std::shared_ptr<Context> context2 = GetParam()->makeContext();
  constexpr auto dataSize = 256;
  Queue<Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = context1->createChannel(
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
        auto channel = context2->createChannel(
            std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Perform send and wait for completion.
        std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
        std::future<Error> future;
        std::tie(descriptorFuture, future) =
            sendWithFuture(channel, data.data(), data.size());
        Error error;
        Channel::TDescriptor descriptor;
        std::tie(error, descriptor) = descriptorFuture.get();
        ASSERT_FALSE(error);
        descriptorQueue.push(std::move(descriptor));
        ASSERT_FALSE(future.get());
      });

  context1->join();
  context2->join();
}

TEST_P(ChannelTest, SendMultipleTensors) {
  std::shared_ptr<Context> context1 = GetParam()->makeContext();
  std::shared_ptr<Context> context2 = GetParam()->makeContext();
  constexpr auto dataSize = 256 * 1024; // 256KB
  Queue<Channel::TDescriptor> descriptorQueue;
  constexpr int numTensors = 100;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = context1->createChannel(
            std::move(conn), Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Error futures
        std::vector<std::future<Error>> futures;

        // Perform send and wait for completion.
        for (int i = 0; i < numTensors; i++) {
          std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
          std::future<Error> future;
          std::tie(descriptorFuture, future) =
              sendWithFuture(channel, data.data(), data.size());
          Error error;
          Channel::TDescriptor descriptor;
          std::tie(error, descriptor) = descriptorFuture.get();
          ASSERT_FALSE(error);
          descriptorQueue.push(std::move(descriptor));
          futures.push_back(std::move(future));
        }
        for (auto& future : futures) {
          ASSERT_FALSE(future.get());
        }
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = context2->createChannel(
            std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with zeroes.
        std::vector<std::vector<uint8_t>> dataVec(
            numTensors, std::vector<uint8_t>(dataSize, 0));

        // Error futures
        std::vector<std::future<Error>> futures;

        // Perform recv and wait for completion.
        for (int i = 0; i < numTensors; i++) {
          std::future<Error> future = recvWithFuture(
              channel, descriptorQueue.pop(), dataVec[i].data(), dataSize);
          futures.push_back(std::move(future));
        }
        for (auto& future : futures) {
          ASSERT_FALSE(future.get());
        }

        // Validate contents of vector.
        for (auto& data : dataVec) {
          for (int i = 0; i < data.size(); i++) {
            uint8_t value = i;
            EXPECT_EQ(data[i], value);
          }
        }
      });

  context1->join();
  context2->join();
}

TEST_P(ChannelTest, contextIsNotJoined) {
  std::shared_ptr<Context> context = GetParam()->makeContext();

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        context->createChannel(std::move(conn), Channel::Endpoint::kListen);
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        context->createChannel(std::move(conn), Channel::Endpoint::kConnect);
      });
}

TEST_P(ChannelTest, CallbacksAreDeferred) {
  // This test wants to make sure that the "heavy lifting" of copying data isn't
  // performed inline inside the recv method as that would make the user-facing
  // read method of the pipe blocking. However, since we can't really check that
  // behavior, we'll check a highly correlated one: that the recv callback isn't
  // called inline from within the recv method. We do so by having that behavior
  // cause a deadlock.
  std::shared_ptr<Context> context1 = GetParam()->makeContext();
  std::shared_ptr<Context> context2 = GetParam()->makeContext();
  constexpr auto dataSize = 256;
  Queue<Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = context1->createChannel(
            std::move(conn), Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Perform send and wait for completion.
        std::promise<std::tuple<Error, Channel::TDescriptor>> descriptorPromise;
        std::promise<Error> promise;
        std::mutex mutex;
        std::unique_lock<std::mutex> callerLock(mutex);
        channel->send(
            data.data(),
            data.size(),
            [&descriptorPromise](
                const Error& error, Channel::TDescriptor descriptor) {
              descriptorPromise.set_value(
                  std::make_tuple(error, std::move(descriptor)));
            },
            [&promise, &mutex](const Error& error) {
              std::unique_lock<std::mutex> calleeLock(mutex);
              promise.set_value(error);
            });
        callerLock.unlock();
        Error error;
        Channel::TDescriptor descriptor;
        std::tie(error, descriptor) = descriptorPromise.get_future().get();
        ASSERT_FALSE(error);
        descriptorQueue.push(std::move(descriptor));
        ASSERT_FALSE(promise.get_future().get());
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = context2->createChannel(
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

  context1->join();
  context2->join();
}
