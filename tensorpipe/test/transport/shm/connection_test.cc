/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/shm/shm_test.h>

#include <gtest/gtest.h>
#include <nop/serializer.h>
#include <nop/structure.h>

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

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        doRead(
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
              peers_->done(PeerGroup::kServer);
            });
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        doWrite(
            conn,
            srcBuf.c_str(),
            srcBuf.length(),
            [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              peers_->done(PeerGroup::kClient);
            });
        peers_->join(PeerGroup::kClient);
      });
}

TEST_P(ShmTransportTest, ChunkingImplicitRead) {
  // This is larger than the default ring buffer size.
  const size_t kMsgSize = 5 * kBufferSize;
  std::string msg(kMsgSize, 0x42);

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        doRead(
            conn, [&, conn](const Error& error, const void* ptr, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, kMsgSize);
              for (int i = 0; i < kMsgSize; ++i) {
                ASSERT_EQ(static_cast<const uint8_t*>(ptr)[i], msg[i]);
              }
              peers_->done(PeerGroup::kServer);
            });
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        doWrite(conn, msg.c_str(), msg.length(), [&, conn](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          peers_->done(PeerGroup::kClient);
        });
        peers_->join(PeerGroup::kClient);
      });
}

TEST_P(ShmTransportTest, QueueWrites) {
  // This is large enough that two of those will not fit in the ring buffer at
  // the same time.
  constexpr int numMsg = 2;
  constexpr size_t numBytes = (3 * kBufferSize) / 4;
  const std::string kReady = "ready";
  std::array<char, numBytes> garbage;

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        // Wait for peer to queue up writes before attempting to read
        EXPECT_EQ(kReady, peers_->recv(PeerGroup::kServer));

        for (int i = 0; i < numMsg; ++i) {
          doRead(
              conn,
              [&, conn, i](const Error& error, const void* ptr, size_t len) {
                ASSERT_FALSE(error) << error.what();
                ASSERT_EQ(len, numBytes);
                if (i == numMsg - 1) {
                  peers_->done(PeerGroup::kServer);
                }
              });
        }
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; ++i) {
          doWrite(
              conn,
              garbage.data(),
              garbage.size(),
              [&, conn, i](const Error& error) {
                ASSERT_FALSE(error) << error.what();
                if (i == numMsg - 1) {
                  peers_->done(PeerGroup::kClient);
                }
              });
        }
        peers_->send(PeerGroup::kServer, kReady);
        peers_->join(PeerGroup::kClient);
      });
}

namespace {

struct MyNopType {
  std::string myStringField;
  NOP_STRUCTURE(MyNopType, myStringField);
};

} // namespace

TEST_P(ShmTransportTest, NopWriteWrapAround) {
  constexpr int numMsg = 2;
  constexpr size_t kSize = (3 * kBufferSize) / 4;

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; ++i) {
          auto holder = std::make_shared<NopHolder<MyNopType>>();
          conn->read(*holder, [&, conn, holder, i](const Error& error) {
            ASSERT_FALSE(error) << error.what();
            ASSERT_EQ(holder->getObject().myStringField.length(), kSize);
            if (i == numMsg - 1) {
              peers_->done(PeerGroup::kServer);
            }
          });
        }
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; ++i) {
          auto holder = std::make_shared<NopHolder<MyNopType>>();
          holder->getObject().myStringField = std::string(kSize, 'B');
          conn->write(*holder, [&, conn, holder, i](const Error& error) {
            ASSERT_FALSE(error) << error.what();
            if (i == numMsg - 1) {
              peers_->done(PeerGroup::kClient);
            }
          });
        }
        peers_->join(PeerGroup::kClient);
      });
}

INSTANTIATE_TEST_CASE_P(Shm, ShmTransportTest, ::testing::Values(&helper));
