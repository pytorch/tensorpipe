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

#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/listener.h>

#include <gtest/gtest.h>

class TransportTestHelper {
 public:
  virtual std::shared_ptr<tensorpipe::transport::Context> getContext() = 0;
  virtual std::string defaultAddr() = 0;
  virtual ~TransportTestHelper() = default;
};

class TransportTest : public ::testing::TestWithParam<TransportTestHelper*> {
 public:
  void test_connection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          listeningFn,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          connectingFn) {
    using namespace tensorpipe::transport;

    auto ctx = GetParam()->getContext();
    auto addr = GetParam()->defaultAddr();
    {
      auto listener = ctx->listen(addr);
      tensorpipe::Queue<std::shared_ptr<Connection>> queue;
      listener->accept([&](const tensorpipe::Error& error,
                           std::shared_ptr<Connection> conn) {
        ASSERT_FALSE(error) << error.what();
        queue.push(std::move(conn));
      });

      // Start thread for listening side.
      std::thread listeningThread([&]() { listeningFn(queue.pop()); });

      // Capture real listener address.
      const std::string listenerAddr = listener->addr();

      // Start thread for connecting side.
      std::thread connectingThread(
          [&]() { connectingFn(ctx->connect(listenerAddr)); });

      // Wait for completion.
      listeningThread.join();
      connectingThread.join();
    }

    ctx->join();
  }
};
