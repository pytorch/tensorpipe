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
        conn->read([&, conn](const Error& error, const void* ptr, size_t len) {
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
