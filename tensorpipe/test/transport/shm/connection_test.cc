/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "../connection_test.h"

using namespace tensorpipe::transport;

using SHMConnectionTest = ConnectionTest<SHMConnectionTestHelper>;

TEST_F(SHMConnectionTest, LargeWrite) {
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
