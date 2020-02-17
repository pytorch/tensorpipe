/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/shm/connection_test.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

using SHMConnectionTest = ConnectionTest<SHMConnectionTestHelper>;

// NOTE: This test is disabled until chunking is implemented.
TEST_F(SHMConnectionTest, DISABLED_LargeWrite) {
  // This is larger than the default ring buffer size.
  const int kMsgSize = 2 * shm::Connection::kDefaultSize;
  std::string msg(kMsgSize, 0x42);

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        conn->read([conn](
                       const Error& error,
                       const void* /* unused */,
                       size_t /* unused */) {
          ASSERT_TRUE(error);
          ASSERT_EQ(error.what(), "eof");
        });
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->write(msg.c_str(), msg.length(), [conn](const Error& error) {
          ASSERT_TRUE(error);
          ASSERT_EQ(error.what().substr(0, 11), "short write");
        });
      });
}

TEST_F(SHMConnectionTest, QueueWrites) {
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
