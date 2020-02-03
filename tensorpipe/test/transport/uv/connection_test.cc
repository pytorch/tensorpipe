/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

namespace {

void initializePeers(
    std::shared_ptr<uv::Loop> loop,
    uv::Sockaddr addr,
    std::function<void(std::shared_ptr<Connection>)> listeningFn,
    std::function<void(std::shared_ptr<Connection>)> connectingFn) {
  Queue<std::shared_ptr<Connection>> queue;

  // Listener runs callback for every new connection.
  // We only care about a single one for tests.
  auto listener = uv::Listener::create(loop, addr);
  listener->accept([&](const Error& error, std::shared_ptr<Connection> conn) {
    ASSERT_FALSE(error) << error.what();
    queue.push(std::move(conn));
  });

  // Capture real listener address.
  auto listenerAddr = listener->sockaddr();

  // Start thread for listening side.
  std::thread listeningThread([&]() { listeningFn(queue.pop()); });

  // Start thread for connecting side.
  std::thread connectingThread(
      [&]() { connectingFn(uv::Connection::create(loop, listenerAddr)); });

  // Wait for completion.
  listeningThread.join();
  connectingThread.join();
}

} // namespace

TEST(Connection, Initialization) {
  auto loop = uv::Loop::create();
  auto addr = uv::Sockaddr::createInetSockAddr("127.0.0.1");
  constexpr size_t numBytes = 13;

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        Queue<size_t> reads;
        conn->read([&](const Error& error, const void* ptr, size_t len) {
          ASSERT_FALSE(error) << error.what();
          reads.push(len);
        });
        // Wait for the read callback to be called.
        ASSERT_EQ(numBytes, reads.pop());
      },
      [&](std::shared_ptr<Connection> conn) {
        Queue<bool> writes;
        std::array<char, numBytes> garbage;
        conn->write(garbage.data(), garbage.size(), [&](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          writes.push(true);
        });
        // Wait for the write callback to be called.
        writes.pop();
      });

  loop->join();
}

TEST(Connection, InitializationError) {
  auto loop = uv::Loop::create();
  auto addr = uv::Sockaddr::createInetSockAddr("127.0.0.1");

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        Queue<Error> errors;
        conn->read([&](const Error& error, const void* ptr, size_t len) {
          errors.push(error);
        });
        auto error = errors.pop();
        ASSERT_TRUE(error);
      });

  loop->join();
}

TEST(Connection, DestroyConnectionFromCallback) {
  auto loop = uv::Loop::create();
  auto addr = uv::Sockaddr::createInetSockAddr("127.0.0.1");

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        // This should be the only connection instance.
        EXPECT_EQ(conn.use_count(), 1);
        // Move connection instance to lambda scope, so we can destroy
        // the only instance we have from the callback itself. This
        // tests that the transport keeps the connection alive as long
        // as it's executing a callback.
        conn->read([conn](
                       const Error& /* unused */,
                       const void* /* unused */,
                       size_t /* unused */) mutable {
          // Destroy connection from within callback.
          EXPECT_GT(conn.use_count(), 1);
          conn.reset();
        });
      });

  loop->join();
}

TEST(Connection, LargeWrite) {
  auto loop = uv::Loop::create();
  auto addr = uv::Sockaddr::createInetSockAddr("127.0.0.1");

  constexpr int kMsgSize = 16 * 1024 * 1024;
  std::string msg(kMsgSize, 0x42);

  initializePeers(
      loop,
      addr,
      [&](std::shared_ptr<Connection> conn) {
        conn->write(msg.c_str(), msg.length(), [conn](const Error& error) {
          ASSERT_FALSE(error) << error.what();
        });
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->read([&, conn](const Error& error, const void* data, size_t len) {
          ASSERT_FALSE(error) << error.what();
          ASSERT_EQ(len, msg.length());
          const char* cdata = (const char*)data;
          for (int i = 0; i < len; ++i) {
            const char c = cdata[i];
            ASSERT_EQ(c, msg[i])
                << "Wrong value at position " << i << " of " << msg.length();
          }
        });
      });

  loop->join();
}
