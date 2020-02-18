/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <thread>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/uv/context.h>

#include <gtest/gtest.h>

void testConnectionPair(
    std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)> f1,
    std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
        f2) {
  auto context = std::make_shared<tensorpipe::transport::uv::Context>();
  auto addr = "127.0.0.1";

  {
    tensorpipe::Queue<std::shared_ptr<tensorpipe::transport::Connection>> q1,
        q2;

    // Listening side.
    auto listener = context->listen(addr);
    listener->accept(
        [&](const tensorpipe::Error& error,
            std::shared_ptr<tensorpipe::transport::Connection> connection) {
          ASSERT_FALSE(error) << error.what();
          q1.push(std::move(connection));
        });

    // Connecting side.
    q2.push(context->connect(listener->addr()));

    // Run user specified functions.
    std::thread t1([&] { f1(q1.pop()); });
    std::thread t2([&] { f2(q2.pop()); });
    t1.join();
    t2.join();
  }

  context->join();
}

[[nodiscard]] std::pair<
    tensorpipe::channel::Channel::TDescriptor,
    std::future<tensorpipe::Error>>
sendWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel> channel,
    const void* ptr,
    size_t length) {
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = promise->get_future();
  auto descriptor = channel->send(
      ptr,
      length,
      [promise{std::move(promise)}](const tensorpipe::Error& error) {
        promise->set_value(error);
      });
  return {std::move(descriptor), std::move(future)};
}

[[nodiscard]] std::future<tensorpipe::Error> recvWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel> channel,
    tensorpipe::channel::Channel::TDescriptor descriptor,
    void* ptr,
    size_t length) {
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = promise->get_future();
  channel->recv(
      std::move(descriptor),
      ptr,
      length,
      [promise{std::move(promise)}](const tensorpipe::Error& error) {
        promise->set_value(error);
      });
  return future;
}

template <typename T>
class ChannelFactoryTest : public ::testing::Test {};

TYPED_TEST_CASE_P(ChannelFactoryTest);

TYPED_TEST_P(ChannelFactoryTest, DomainDescriptor) {
  std::shared_ptr<tensorpipe::channel::ChannelFactory> factory1 =
      std::make_shared<TypeParam>();
  std::shared_ptr<tensorpipe::channel::ChannelFactory> factory2 =
      std::make_shared<TypeParam>();
  EXPECT_FALSE(factory1->domainDescriptor().empty());
  EXPECT_FALSE(factory2->domainDescriptor().empty());
  EXPECT_EQ(factory1->domainDescriptor(), factory2->domainDescriptor());
}

TYPED_TEST_P(ChannelFactoryTest, CreateChannel) {
  std::shared_ptr<tensorpipe::channel::ChannelFactory> factory1 =
      std::make_shared<TypeParam>();
  std::shared_ptr<tensorpipe::channel::ChannelFactory> factory2 =
      std::make_shared<TypeParam>();
  constexpr auto dataSize = 256;
  tensorpipe::Queue<tensorpipe::channel::Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<tensorpipe::transport::Connection> conn) {
        auto channel = factory1->createChannel(
            std::move(conn), tensorpipe::channel::Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(256);
        for (auto i = 0; i < data.size(); i++) {
          data[i] = i;
        }

        // Perform send and wait for completion.
        tensorpipe::channel::Channel::TDescriptor descriptor;
        std::future<tensorpipe::Error> future;
        std::tie(descriptor, future) =
            sendWithFuture(channel, data.data(), data.size());
        descriptorQueue.push(std::move(descriptor));
        ASSERT_FALSE(future.get());
      },
      [&](std::shared_ptr<tensorpipe::transport::Connection> conn) {
        auto channel = factory2->createChannel(
            std::move(conn), tensorpipe::channel::Channel::Endpoint::kConnect);

        // Initialize with zeroes.
        std::vector<uint8_t> data(256);
        for (auto i = 0; i < data.size(); i++) {
          data[i] = 0;
        }

        // Perform recv and wait for completion.
        std::future<tensorpipe::Error> future = recvWithFuture(
            channel, descriptorQueue.pop(), data.data(), data.size());
        ASSERT_FALSE(future.get());

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }
      });
}

REGISTER_TYPED_TEST_CASE_P(ChannelFactoryTest, DomainDescriptor, CreateChannel);
