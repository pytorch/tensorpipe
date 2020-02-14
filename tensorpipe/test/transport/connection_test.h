/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <thread>

#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

#include <gtest/gtest.h>

class ConnectionTestHelper {
 public:
  virtual ~ConnectionTestHelper() = default;
  virtual std::shared_ptr<tensorpipe::transport::Listener> getListener() = 0;
  virtual std::shared_ptr<tensorpipe::transport::Connection> connect(
      const std::string& addr) = 0;
};

template <class T>
class ConnectionTest : public ::testing::Test {
 public:
  void test_connection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          listeningFn,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          connectingFn) {
    using namespace tensorpipe::transport;

    std::unique_ptr<ConnectionTestHelper> helper = std::make_unique<T>();

    auto listener = helper->getListener();
    tensorpipe::Queue<std::shared_ptr<Connection>> queue;
    listener->accept(
        [&](const tensorpipe::Error& error, std::shared_ptr<Connection> conn) {
          ASSERT_FALSE(error) << error.what();
          queue.push(std::move(conn));
        });

    // Start thread for listening side.
    std::thread listeningThread([&]() { listeningFn(queue.pop()); });

    // Capture real listener address.
    const std::string listenerAddr = listener->addr();

    // Start thread for connecting side.
    std::thread connectingThread(
        [&]() { connectingFn(helper->connect(listenerAddr)); });

    // Wait for completion.
    listeningThread.join();
    connectingThread.join();
  }
};

class ConnectionTypeNames {
 public:
  template <class T>
  static std::string GetName(int /* unused */) {
    return T::transportName();
  }
};
