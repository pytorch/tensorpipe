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
  virtual ~ConnectionTestHelper() = default;
  virtual std::shared_ptr<Listener> getListener() = 0;
  virtual std::shared_ptr<Connection> connect(const std::string& addr) = 0;
};

class SHMConnectionTestHelper : public ConnectionTestHelper {
 public:
  SHMConnectionTestHelper() : loop_(shm::Loop::create()) {}

  ~SHMConnectionTestHelper() override {
    loop_->join();
  }

  std::shared_ptr<Listener> getListener() override {
    auto addr = shm::Sockaddr::createAbstractUnixAddr(kUnixAddr);
    return shm::Listener::create(loop_, addr);
  }

  std::shared_ptr<Connection> connect(const std::string& addr) override {
    auto socket = shm::Socket::createForFamily(AF_UNIX);
    auto saddr = shm::Sockaddr::createAbstractUnixAddr(addr);
    socket->connect(saddr);
    return shm::Connection::create(loop_, std::move(socket));
  }

  static std::string transportName() {
    return "shm";
  }

 private:
  const std::string kUnixAddr = "foobar";
  std::shared_ptr<shm::Loop> loop_;
};

class UVConnectionTestHelper : public ConnectionTestHelper {
 public:
  UVConnectionTestHelper() : loop_(uv::Loop::create()) {}

  ~UVConnectionTestHelper() override {
    loop_->join();
  }

  std::shared_ptr<Listener> getListener() override {
    auto addr = uv::Sockaddr::createInetSockAddr(kIPAddr);
    return uv::Listener::create(loop_, addr);
  }

  std::shared_ptr<Connection> connect(const std::string& addr) override {
    auto saddr = uv::Sockaddr::createInetSockAddr(addr);
    return uv::Connection::create(loop_, saddr);
  }

  static std::string transportName() {
    return "uv";
  }

 private:
  const std::string kIPAddr = "127.0.0.1";
  std::shared_ptr<uv::Loop> loop_;
};

} // namespace

template <class T>
class ConnectionTest : public ::testing::Test {
 public:
  void test_connection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          listeningFn,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          connectingFn) {
    std::unique_ptr<ConnectionTestHelper> helper(new T);

    auto listener = helper->getListener();
    tensorpipe::Queue<std::shared_ptr<Connection>> queue;
    listener->accept([&](const Error& error, std::shared_ptr<Connection> conn) {
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

using ConnectionTypes =
    ::testing::Types<SHMConnectionTestHelper, UVConnectionTestHelper>;

class ConnectionTypeNames {
 public:
  template <class T>
  static std::string GetName(int /* unused */) {
    return T::transportName();
  }
};

// TYPED_TEST_SUITE(ConnectionTest, ConnectionTypes, ConnectionTypeNames);
TYPED_TEST_CASE(ConnectionTest, ConnectionTypes);

#endif
