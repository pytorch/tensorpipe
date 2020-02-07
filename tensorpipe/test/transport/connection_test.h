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

class ConnectionTestHelper {
 public:
  std::shared_ptr<Listener> listener_;

  virtual ~ConnectionTestHelper() = default;

  std::shared_ptr<Connection> accept() {
    tensorpipe::Queue<std::shared_ptr<Connection>> queue;
    listener_->accept(
        [&](const Error& error, std::shared_ptr<Connection> conn) {
          ASSERT_FALSE(error) << error.what();
          queue.push(std::move(conn));
        });

    return queue.pop();
  }

  virtual std::shared_ptr<Connection> connect() = 0;
};

class SHMConnectionTestHelper : public ConnectionTestHelper {
 public:
  SHMConnectionTestHelper()
      : loop_(shm::Loop::create()),
        addr_(shm::Sockaddr::createAbstractUnixAddr("foobar")) {
    listener_ = shm::Listener::create(loop_, addr_);
  }

  ~SHMConnectionTestHelper() override {
    loop_->join();
  }

 private:
  std::shared_ptr<shm::Loop> loop_;
  const shm::Sockaddr addr_;

  std::shared_ptr<Connection> connect() override {
    auto socket = shm::Socket::createForFamily(AF_UNIX);
    socket->connect(addr_);

    return shm::Connection::create(loop_, std::move(socket));
  }
};

class UVConnectionTestHelper : public ConnectionTestHelper {
 public:
  UVConnectionTestHelper() : loop_(uv::Loop::create()) {
    auto addr = uv::Sockaddr::createInetSockAddr("127.0.0.1");
    listener_ = uv::Listener::create(loop_, addr);
  }

  ~UVConnectionTestHelper() override {
    loop_->join();
  }

 private:
  std::shared_ptr<uv::Loop> loop_;

  std::shared_ptr<Connection> connect() override {
    // Capture real listener address.
    auto addr = std::dynamic_pointer_cast<uv::Listener>(listener_)->sockaddr();
    return uv::Connection::create(loop_, addr);
  }
};

} // namespace

template <typename T>
ConnectionTestHelper* getHelper();

template <>
ConnectionTestHelper* getHelper<tensorpipe::transport::shm::Connection>() {
  return new SHMConnectionTestHelper;
}

template <>
ConnectionTestHelper* getHelper<tensorpipe::transport::uv::Connection>() {
  return new UVConnectionTestHelper;
}

template <class T>
class ConnectionTest : public testing::Test {
 public:
  ConnectionTest() : helper_(getHelper<T>()) {}

  void test_connection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          listeningFn,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          connectingFn) {
    // Start thread for listening side.
    std::thread listeningThread([&]() { listeningFn(helper_->accept()); });

    // Start thread for connecting side.
    std::thread connectingThread([&]() { connectingFn(helper_->connect()); });

    // Wait for completion.
    listeningThread.join();
    connectingThread.join();
  }

 private:
  std::unique_ptr<ConnectionTestHelper> helper_;
};


using ConnectionTypes = ::testing::Types<
    tensorpipe::transport::shm::Connection,
    tensorpipe::transport::uv::Connection>;
TYPED_TEST_SUITE(ConnectionTest, ConnectionTypes);

#endif
