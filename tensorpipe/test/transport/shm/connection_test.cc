/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/listener.h>
#include <tensorpipe/transport/shm/loop.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

namespace {

using tensorpipe::Queue;

void initializePeers(
    std::shared_ptr<shm::Loop> loop,
    shm::Sockaddr addr,
    std::function<void(std::shared_ptr<Connection>)> listeningFn,
    std::function<void(std::shared_ptr<Connection>)> connectingFn) {
  Queue<std::shared_ptr<Connection>> queue;

  // Listener runs callback for every new connection.
  // We only care about a single one for tests.
  auto listener = shm::Listener::create(loop, addr);
  listener->accept([&](const Error& error, std::shared_ptr<Connection> conn) {
    ASSERT_FALSE(error) << error.what();
    queue.push(std::move(conn));
  });

  // Start thread for listening side.
  std::thread listeningThread([&]() { listeningFn(queue.pop()); });

  // Start thread for connecting side.
  std::thread connectingThread([&]() {
    auto socket = shm::Socket::createForFamily(AF_UNIX);
    socket->connect(addr);
    connectingFn(shm::Connection::create(loop, std::move(socket)));
  });

  // Wait for completion.
  listeningThread.join();
  connectingThread.join();
}

} // namespace

TEST(Connection, Initialization) {
  auto loop = shm::Loop::create();
  auto addr = shm::Sockaddr::createAbstractUnixAddr("foobar");
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        conn->read([&, conn](const Error& error, const void* ptr, size_t len) {
          ASSERT_FALSE(error) << error.what();
          ASSERT_EQ(numBytes, garbage.size());
        });
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->write(
            garbage.data(), garbage.size(), [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
            });
      });

  loop->join();
}

TEST(Connection, InitializationError) {
  auto loop = shm::Loop::create();
  auto addr = shm::Sockaddr::createAbstractUnixAddr("foobar");

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->read([conn](const Error& error, const void* ptr, size_t len) {
          ASSERT_TRUE(error);
        });
      });

  loop->join();
}

TEST(Connection, LargeWrite) {
  auto loop = shm::Loop::create();
  auto addr = shm::Sockaddr::createAbstractUnixAddr("foobar");

  // This is larger than the default ring buffer size.
  const int kMsgSize = 2 * shm::Connection::kDefaultSize;
  std::string msg(kMsgSize, 0x42);

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->write(msg.c_str(), msg.length(), [conn](const Error& error) {
          ASSERT_TRUE(error);
        });
      });

  loop->join();
}
