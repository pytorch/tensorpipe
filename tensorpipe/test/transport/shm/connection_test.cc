/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/shm/shm_test.h>

#include <tensorpipe/proto/core.pb.h>
#include <tensorpipe/transport/shm/connection.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

TEST_P(TransportTest, Chunking) {
  // This is larger than the default ring buffer size.
  const int kMsgSize = 5 * shm::Connection::kDefaultSize;
  std::string srcBuf(kMsgSize, 0x42);
  auto dstBuf = std::make_unique<char[]>(kMsgSize);
  // Using direct initialization since std::atomic is not copy constructible.
  std::atomic<int> nbCallbackCalls{0};

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        conn->read(
            dstBuf.get(),
            kMsgSize,
            [&, conn](const Error& error, const void* ptr, size_t len) {
              ++nbCallbackCalls;
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, kMsgSize);
              ASSERT_EQ(ptr, dstBuf.get());
              for (int i = 0; i < kMsgSize; ++i) {
                ASSERT_EQ(dstBuf[i], srcBuf[i]);
              }
            });
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->write(
            srcBuf.c_str(), srcBuf.length(), [&, conn](const Error& error) {
              ++nbCallbackCalls;
              ASSERT_FALSE(error) << error.what();
            });
      });

  ASSERT_EQ(nbCallbackCalls, 2);
}

TEST_P(TransportTest, ChunkingImplicitRead) {
  // This is larger than the default ring buffer size.
  const size_t kMsgSize = 5 * shm::Connection::kDefaultSize;
  std::string msg(kMsgSize, 0x42);
  std::atomic<int> nbCallbackCalls{0};

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        conn->read([&, conn](const Error& error, const void* ptr, size_t len) {
          ++nbCallbackCalls;
          ASSERT_FALSE(error) << error.what();
          ASSERT_EQ(len, kMsgSize);
          for (int i = 0; i < kMsgSize; ++i) {
            ASSERT_EQ(static_cast<const uint8_t*>(ptr)[i], msg[i]);
          }
        });
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->write(msg.c_str(), msg.length(), [&, conn](const Error& error) {
          ++nbCallbackCalls;
          ASSERT_FALSE(error) << error.what();
        });
      });

  ASSERT_EQ(nbCallbackCalls, 2);
}

TEST_P(TransportTest, QueueWrites) {
  // This is large enough that two of those will not fit in the ring buffer at
  // the same time.
  constexpr size_t numBytes = (3 * shm::Connection::kDefaultSize) / 4;
  std::array<char, numBytes> garbage;

  tensorpipe::Queue<bool> queue;

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        // Wait before both writes have been registered before attempting to
        // read.
        queue.pop();

        for (int i = 0; i < 2; ++i) {
          conn->read(
              [&, conn](const Error& error, const void* ptr, size_t len) {
                ASSERT_FALSE(error) << error.what();
                ASSERT_EQ(len, numBytes);
              });
        }
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < 2; ++i) {
          conn->write(
              garbage.data(), garbage.size(), [conn](const Error& error) {
                ASSERT_FALSE(error) << error.what();
              });
        }

        queue.push(true);
      });
}

TEST_P(TransportTest, ProtobufWriteWrapAround) {
  constexpr size_t kSize = (3 * shm::Connection::kDefaultSize) / 4;

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < 2; ++i) {
          auto message =
              std::make_shared<tensorpipe::proto::ChannelAdvertisement>();
          conn->read(*message, [&, conn, message](const Error& error) {
            ASSERT_FALSE(error) << error.what();
            ASSERT_EQ(message->domain_descriptor().length(), kSize);
          });
        }
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < 2; ++i) {
          auto message =
              std::make_shared<tensorpipe::proto::ChannelAdvertisement>();
          message->set_domain_descriptor(std::string(kSize, 'B'));
          conn->write(*message, [conn, message](const Error& error) {
            ASSERT_FALSE(error) << error.what();
          });
        }
      });
}
