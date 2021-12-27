/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/transport_test.h>

#include <array>

#include <nop/serializer.h>
#include <nop/structure.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

TEST_P(TransportTest, Connection_Initialization) {
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        doRead(
            conn,
            [&](const Error& error, const void* /* unused */, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, garbage.size());
              peers_->done(PeerGroup::kServer);
            });
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        doWrite(conn, garbage.data(), garbage.size(), [&](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          peers_->done(PeerGroup::kClient);
        });
        peers_->join(PeerGroup::kClient);
      });
}

TEST_P(TransportTest, Connection_InitializationError) {
  int numRequests = 10;

  testConnection(
      [&](std::shared_ptr<Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numRequests; i++) {
          std::promise<void> readCompletedProm;
          doRead(
              conn,
              [&, conn](
                  const Error& error,
                  const void* /* unused */,
                  size_t /* unused */) {
                ASSERT_TRUE(error);
                readCompletedProm.set_value();
              });
          readCompletedProm.get_future().wait();
        }
      });
}

// Disabled because no one really knows what this test was meant to check.
TEST_P(TransportTest, DISABLED_Connection_DestroyConnectionFromCallback) {
  testConnection(
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
        doRead(
            conn,
            [conn](
                const Error& /* unused */,
                const void* /* unused */,
                size_t /* unused */) mutable {
              // Destroy connection from within callback.
              EXPECT_GT(conn.use_count(), 1);
              conn.reset();
            });
      });
}

namespace {

struct MyNopType {
  uint32_t myIntField;
  NOP_STRUCTURE(MyNopType, myIntField);
};

} // namespace

TEST_P(TransportTest, Connection_NopWrite) {
  constexpr size_t kSize = 0x42;

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        auto holder = std::make_shared<NopHolder<MyNopType>>();
        MyNopType& object = holder->getObject();
        conn->read(*holder, [&, conn, holder](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          ASSERT_EQ(object.myIntField, kSize);
          peers_->done(PeerGroup::kServer);
        });
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        auto holder = std::make_shared<NopHolder<MyNopType>>();
        MyNopType& object = holder->getObject();
        object.myIntField = kSize;
        conn->write(*holder, [&, conn, holder](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          peers_->done(PeerGroup::kClient);
        });
        peers_->join(PeerGroup::kClient);
      });
}

TEST_P(TransportTest, Connection_QueueWritesBeforeReads) {
  constexpr int kMsgSize = 16 * 1024;
  constexpr int numMsg = 10;
  const std::string kReady = "ready";
  std::string msg[numMsg];

  for (int i = 0; i < numMsg; i++) {
    msg[i] = std::string(kMsgSize, static_cast<char>(i));
  }

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; i++) {
          doWrite(
              conn,
              msg[i].c_str(),
              msg[i].length(),
              [&, conn, i](const Error& error) {
                ASSERT_FALSE(error) << error.what();
                if (i == numMsg - 1) {
                  peers_->send(PeerGroup::kClient, kReady);
                  peers_->done(PeerGroup::kServer);
                }
              });
        }
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        ASSERT_EQ(kReady, peers_->recv(PeerGroup::kClient));
        for (int i = 0; i < numMsg; i++) {
          doRead(
              conn,
              [&, conn, i](const Error& error, const void* data, size_t len) {
                ASSERT_FALSE(error) << error.what();
                ASSERT_EQ(len, msg[i].length());
                const char* cdata = (const char*)data;
                for (int j = 0; j < len; ++j) {
                  const char c = cdata[j];
                  ASSERT_EQ(c, msg[i][j]) << "Wrong value at position " << j
                                          << " of " << msg[i].length();
                }
                if (i == numMsg - 1) {
                  peers_->done(PeerGroup::kClient);
                }
              });
        }
        peers_->join(PeerGroup::kClient);
      });
}

// TODO: Enable this test when uv transport could handle
TEST_P(TransportTest, DISABLED_Connection_EmptyBuffer) {
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;
  int ioNum = 100;

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        std::atomic<int> n(ioNum);
        for (int i = 0; i < ioNum; i++) {
          if (i % 2 == 0) {
            // Empty buffer
            doRead(
                conn,
                nullptr,
                0,
                [&, conn](const Error& error, const void* ptr, size_t len) {
                  ASSERT_FALSE(error) << error.what();
                  ASSERT_EQ(len, 0);
                  ASSERT_EQ(ptr, nullptr);
                  if (--n == 0) {
                    peers_->done(PeerGroup::kServer);
                  }
                });
          } else {
            // Garbage buffer
            doRead(
                conn,
                [&, conn](
                    const Error& error, const void* /* unused */, size_t len) {
                  ASSERT_FALSE(error) << error.what();
                  ASSERT_EQ(len, garbage.size());
                  if (--n == 0) {
                    peers_->done(PeerGroup::kServer);
                  }
                });
          }
        }

        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        std::atomic<int> n(ioNum);
        for (int i = 0; i < ioNum; i++) {
          if ((i & 1) == 0) {
            // Empty buffer
            doWrite(conn, nullptr, 0, [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              if (--n == 0) {
                peers_->done(PeerGroup::kClient);
              }
            });
          } else {
            // Garbage buffer
            doWrite(
                conn,
                garbage.data(),
                garbage.size(),
                [&, conn](const Error& error) {
                  ASSERT_FALSE(error) << error.what();
                  if (--n == 0) {
                    peers_->done(PeerGroup::kClient);
                  }
                });
          }
        }

        peers_->join(PeerGroup::kClient);
      });
}
