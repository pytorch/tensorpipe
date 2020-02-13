/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/uv/connection_test.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

using UVConnectionTest = ConnectionTest<UVConnectionTestHelper>;

TEST_F(UVConnectionTest, LargeWrite) {
  constexpr int kMsgSize = 16 * 1024 * 1024;
  std::string msg(kMsgSize, 0x42);

  this->test_connection(
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
}
