/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>
#include <thread>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/uv/context.h>

#include <gtest/gtest.h>

class ChannelTestHelper {
 public:
  virtual std::shared_ptr<tensorpipe::channel::Context> makeContext(
      std::string id) = 0;

  virtual ~ChannelTestHelper() = default;
};

class ChannelTest : public ::testing::TestWithParam<ChannelTestHelper*> {
 public:
  void testConnectionPair(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          f1,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          f2) {
    auto context = std::make_shared<tensorpipe::transport::uv::Context>();
    context->setId("harness");
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
      std::future<std::tuple<
          tensorpipe::Error,
          tensorpipe::channel::Channel::TDescriptor>>,
      std::future<tensorpipe::Error>>
  sendWithFuture(
      std::shared_ptr<tensorpipe::channel::Channel> channel,
      const void* ptr,
      size_t length) {
    auto descriptorPromise = std::make_shared<
        std::promise<std::tuple<tensorpipe::Error, std::string>>>();
    auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto descriptorFuture = descriptorPromise->get_future();
    auto future = promise->get_future();
    channel->send(
        ptr,
        length,
        [descriptorPromise{std::move(descriptorPromise)}](
            const tensorpipe::Error& error, std::string descriptor) {
          descriptorPromise->set_value(
              std::make_tuple(error, std::move(descriptor)));
        },
        [promise{std::move(promise)}](const tensorpipe::Error& error) {
          promise->set_value(error);
        });
    return {std::move(descriptorFuture), std::move(future)};
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
};
