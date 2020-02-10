/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "connection_test.h"

#include <tensorpipe/transport/shm/connection.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

TYPED_TEST(ConnectionTest, Initialization) {
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        conn->read(
            [&, conn](
                const Error& error, const void* /* unused */, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, garbage.size());
            });
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->write(
            garbage.data(), garbage.size(), [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
            });
      });
}

TYPED_TEST(ConnectionTest, InitializationError) {
  this->test_connection(
      [&](std::shared_ptr<Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->read([&, conn](
                       const Error& error,
                       const void* /* unused */,
                       size_t /* unused */) { ASSERT_TRUE(error); });
      });
}

TYPED_TEST(ConnectionTest, DestroyConnectionFromCallback) {
  this->test_connection(
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
}
