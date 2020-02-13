/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/basic.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/uv/context.h>

#include <gtest/gtest.h>

using namespace tensorpipe;

namespace {

void testConnectionPair(
    std::function<void(std::shared_ptr<transport::Connection>)> f1,
    std::function<void(std::shared_ptr<transport::Connection>)> f2) {
  auto context = std::make_shared<transport::uv::Context>();
  auto addr = "127.0.0.1";

  {
    Queue<std::shared_ptr<transport::Connection>> q1, q2;

    // Listening side.
    auto listener = context->listen(addr);
    listener->accept([&](const Error& error,
                         std::shared_ptr<transport::Connection> connection) {
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

[[nodiscard]] std::pair<channel::Channel::TDescriptor, std::future<Error>>
sendWithFuture(
    std::shared_ptr<channel::Channel> channel,
    const void* ptr,
    size_t length) {
  auto promise = std::make_shared<std::promise<Error>>();
  auto future = promise->get_future();
  auto descriptor = channel->send(
      ptr, length, [promise{std::move(promise)}](const Error& error) {
        promise->set_value(error);
      });
  return {std::move(descriptor), std::move(future)};
}

[[nodiscard]] std::future<Error> recvWithFuture(
    std::shared_ptr<channel::Channel> channel,
    channel::Channel::TDescriptor descriptor,
    void* ptr,
    size_t length) {
  auto promise = std::make_shared<std::promise<Error>>();
  auto future = promise->get_future();
  channel->recv(
      std::move(descriptor),
      ptr,
      length,
      [promise{std::move(promise)}](const Error& error) {
        promise->set_value(error);
      });
  return future;
}

} // namespace

TEST(BasicChannel, Operation) {
  std::shared_ptr<channel::ChannelFactory> factory1 =
      std::make_shared<channel::basic::BasicChannelFactory>();
  std::shared_ptr<channel::ChannelFactory> factory2 =
      std::make_shared<channel::basic::BasicChannelFactory>();
  constexpr auto dataSize = 256;
  Queue<channel::Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory1->createChannel(std::move(conn));

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        for (auto i = 0; i < data.size(); i++) {
          data[i] = i;
        }

        // Perform send and wait for completion.
        channel::Channel::TDescriptor descriptor;
        std::future<Error> future;
        std::tie(descriptor, future) =
            sendWithFuture(channel, data.data(), data.size());
        descriptorQueue.push(std::move(descriptor));
        ASSERT_FALSE(future.get());
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory2->createChannel(std::move(conn));

        // Initialize with zeroes.
        std::vector<uint8_t> data(dataSize);
        for (auto i = 0; i < data.size(); i++) {
          data[i] = 0;
        }

        // Perform recv and wait for completion.
        std::future<Error> future = recvWithFuture(
            channel, descriptorQueue.pop(), data.data(), data.size());
        ASSERT_FALSE(future.get());

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          ASSERT_EQ(data[i], i);
        }
      });
}
