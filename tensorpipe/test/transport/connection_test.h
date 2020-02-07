/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef CONNECTION_TEST_H
#define CONNECTION_TEST_H

#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/listener.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>

#include <gtest/gtest.h>

namespace {

using namespace tensorpipe::transport;

class ConnectionTester {
 public:
  void test(
      std::function<void(std::shared_ptr<Connection>)> listeningFn,
      std::function<void(std::shared_ptr<Connection>)> connectingFn) {
    tensorpipe::Queue<std::shared_ptr<Connection>> queue;
    listener_->accept(
        [&](const Error& error, std::shared_ptr<Connection> conn) {
          ASSERT_FALSE(error) << error.what();
          queue.push(std::move(conn));
        });

    // Start thread for listening side.
    std::thread listeningThread([&]() { listeningFn(queue.pop()); });

    // Start thread for connecting side.
    std::thread connectingThread([&]() { connectingFn(createConnection()); });

    // Wait for completion.
    listeningThread.join();
    connectingThread.join();
  }

  virtual ~ConnectionTester() = default;

 protected:
  std::shared_ptr<Listener> listener_;

  virtual std::shared_ptr<Connection> createConnection() = 0;
};

class SHMConnectionTester : public ConnectionTester {
 public:
  SHMConnectionTester()
      : loop_(shm::Loop::create()),
        addr_(shm::Sockaddr::createAbstractUnixAddr("foobar")) {
    listener_ = shm::Listener::create(loop_, addr_);
  }

  ~SHMConnectionTester() override {
    loop_->join();
  }

 protected:
  std::shared_ptr<shm::Loop> loop_;
  const shm::Sockaddr addr_;

  std::shared_ptr<Connection> createConnection() override {
    auto socket = shm::Socket::createForFamily(AF_UNIX);
    socket->connect(addr_);

    return shm::Connection::create(loop_, std::move(socket));
  }
};

class UVConnectionTester : public ConnectionTester {
 public:
  UVConnectionTester()
      : loop_(uv::Loop::create()) {
    auto addr = uv::Sockaddr::createInetSockAddr("127.0.0.1");
    listener_ = uv::Listener::create(loop_, addr);
  }

  ~UVConnectionTester() override {
    loop_->join();
  }

 protected:
  std::shared_ptr<uv::Loop> loop_;

  std::shared_ptr<Connection> createConnection() override {
    // Capture real listener address.
    auto addr = std::dynamic_pointer_cast<uv::Listener>(listener_)->sockaddr();
    return uv::Connection::create(loop_, addr);
  }
};

} // namespace

template <typename T>
class ConnectionTest : public ::testing::Test {
 public:
  ConnectionTest() : tester_(new T) {}

  void test_connection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          listeningFn,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          connectingFn) {
    tester_->test(listeningFn, connectingFn);
  }

 private:
  std::unique_ptr<ConnectionTester> tester_;
};

using ConnectionTypes =
    ::testing::Types<SHMConnectionTester, UVConnectionTester>;
TYPED_TEST_SUITE(ConnectionTest, ConnectionTypes);

#endif
