/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/proto/core.pb.h>
#include <tensorpipe/test/transport/shm/shm_test.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

namespace {

class ShmTransportTest : public TransportTest {};

SHMTransportTestHelper helper;

// This value is defined in tensorpipe/transport/shm/connection.h
static constexpr auto kBufferSize = 2 * 1024 * 1024;

} // namespace

TEST_P(ShmTransportTest, Chunking) {
  // This is larger than the default ring buffer size.
  const int kMsgSize = 5 * kBufferSize;
  std::string srcBuf(kMsgSize, 0x42);
  auto dstBuf = std::make_unique<char[]>(kMsgSize);
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        this->doRead(
            conn,
            dstBuf.get(),
            kMsgSize,
            [&, conn](const Error& error, const void* ptr, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, kMsgSize);
              ASSERT_EQ(ptr, dstBuf.get());
              for (int i = 0; i < kMsgSize; ++i) {
                ASSERT_EQ(dstBuf[i], srcBuf[i]);
              }
              readCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        this->doWrite(
            conn,
            srcBuf.c_str(),
            srcBuf.length(),
            [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              writeCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}

TEST_P(ShmTransportTest, ChunkingImplicitRead) {
  // This is larger than the default ring buffer size.
  const size_t kMsgSize = 5 * kBufferSize;
  std::string msg(kMsgSize, 0x42);
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        this->doRead(
            conn, [&, conn](const Error& error, const void* ptr, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, kMsgSize);
              for (int i = 0; i < kMsgSize; ++i) {
                ASSERT_EQ(static_cast<const uint8_t*>(ptr)[i], msg[i]);
              }
              readCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        this->doWrite(
            conn, msg.c_str(), msg.length(), [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              writeCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}

TEST_P(ShmTransportTest, QueueWrites) {
  // This is large enough that two of those will not fit in the ring buffer at
  // the same time.
  constexpr int numMsg = 2;
  constexpr size_t numBytes = (3 * kBufferSize) / 4;
  std::array<char, numBytes> garbage;
  std::promise<void> writeScheduledProm;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        writeScheduledProm.get_future().get();
        for (int i = 0; i < numMsg; ++i) {
          this->doRead(
              conn,
              [&, conn, i](const Error& error, const void* ptr, size_t len) {
                ASSERT_FALSE(error) << error.what();
                ASSERT_EQ(len, numBytes);
                if (i == numMsg - 1) {
                  readCompletedProm.set_value();
                }
              });
        }
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; ++i) {
          this->doWrite(
              conn,
              garbage.data(),
              garbage.size(),
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
      });
}

TEST_P(ShmTransportTest, ProtobufWriteWrapAround) {
  constexpr int numMsg = 2;
  constexpr size_t kSize = (3 * kBufferSize) / 4;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; ++i) {
          auto message =
              std::make_shared<tensorpipe::proto::ChannelAdvertisement>();
          conn->read(*message, [&, conn, message, i](const Error& error) {
            ASSERT_FALSE(error) << error.what();
            ASSERT_EQ(message->domain_descriptor().length(), kSize);
            if (i == numMsg - 1) {
              readCompletedProm.set_value();
            }
          });
        }
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; ++i) {
          auto message =
              std::make_shared<tensorpipe::proto::ChannelAdvertisement>();
          message->set_domain_descriptor(std::string(kSize, 'B'));
          conn->write(*message, [&, conn, message, i](const Error& error) {
            ASSERT_FALSE(error) << error.what();
            if (i == numMsg - 1) {
              writeCompletedProm.set_value();
            }
          });
        }
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}

INSTANTIATE_TEST_CASE_P(Shm, ShmTransportTest, ::testing::Values(&helper));
