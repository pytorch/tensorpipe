/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/transport_test.h>

#include <array>

#include <tensorpipe/proto/core.pb.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

TEST_P(TransportTest, Connection_Initialization) {
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        this->doRead(
            conn,
            [&, conn](
                const Error& error, const void* /* unused */, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, garbage.size());
              readCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        this->doWrite(
            conn,
            garbage.data(),
            garbage.size(),
            [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              writeCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}

TEST_P(TransportTest, Connection_InitializationError) {
  int numRequests = 10;

  this->testConnection(
      [&](std::shared_ptr<Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numRequests; i++) {
          std::promise<void> readCompletedProm;
          this->doRead(
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
  this->testConnection(
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
        this->doRead(
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

TEST_P(TransportTest, Connection_ProtobufWrite) {
  constexpr size_t kSize = 0x42;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        auto message = std::make_shared<
            tensorpipe::proto::MessageDescriptor::PayloadDescriptor>();
        conn->read(*message, [&, conn, message](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          ASSERT_EQ(message->size_in_bytes(), kSize);
          readCompletedProm.set_value();
        });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        auto message = std::make_shared<
            tensorpipe::proto::MessageDescriptor::PayloadDescriptor>();
        message->set_size_in_bytes(kSize);
        conn->write(*message, [&, conn, message](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          writeCompletedProm.set_value();
        });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}

TEST_P(TransportTest, Connection_QueueWritesBeforeReads) {
  constexpr int kMsgSize = 16 * 1024;
  constexpr int numMsg = 10;
  std::string msg[numMsg];

  for (int i = 0; i < numMsg; i++) {
    msg[i] = std::string(kMsgSize, static_cast<char>(i));
  }
  std::promise<void> writeScheduledProm;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; i++) {
          this->doWrite(
              conn,
              msg[i].c_str(),
              msg[i].length(),
              [&, conn, i](const Error& error) {
                ASSERT_FALSE(error) << error.what();
                if (i == numMsg - 1) {
                  writeCompletedProm.set_value();
                }
              });
        }
        writeScheduledProm.set_value();
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        writeScheduledProm.get_future().get();
        for (int i = 0; i < numMsg; i++) {
          this->doRead(
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
                  readCompletedProm.set_value();
                }
              });
        }
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}

// TODO: Enable this test when uv transport could handle
TEST_P(TransportTest, DISABLED_Connection_EmptyBuffer) {
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();
  int ioNum = 100;

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        std::atomic<int> n(ioNum);
        for (int i = 0; i < ioNum; i++) {
          if (i % 2 == 0) {
            // Empty buffer
            this->doRead(
                conn,
                nullptr,
                0,
                [&, conn](const Error& error, const void* ptr, size_t len) {
                  ASSERT_FALSE(error) << error.what();
                  ASSERT_EQ(len, 0);
                  ASSERT_EQ(ptr, nullptr);
                  if (--n == 0) {
                    readCompletedProm.set_value();
                  }
                });
          } else {
            // Garbage buffer
            this->doRead(
                conn,
                [&, conn](
                    const Error& error, const void* /* unused */, size_t len) {
                  ASSERT_FALSE(error) << error.what();
                  ASSERT_EQ(len, garbage.size());
                  if (--n == 0) {
                    readCompletedProm.set_value();
                  }
                });
          }
        }
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        std::atomic<int> n(ioNum);
        for (int i = 0; i < ioNum; i++) {
          if ((i & 1) == 0) {
            // Empty buffer
            this->doWrite(conn, nullptr, 0, [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              if (--n == 0) {
                writeCompletedProm.set_value();
              }
            });
          } else {
            // Garbage buffer
            this->doWrite(
                conn,
                garbage.data(),
                garbage.size(),
                [&, conn](const Error& error) {
                  ASSERT_FALSE(error) << error.what();
                  if (--n == 0) {
                    writeCompletedProm.set_value();
                  }
                });
          }
        }
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}
