/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <tensorpipe/test/transport/uv/uv_test.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

TEST_P(TransportTest, LargeWrite) {
  constexpr int kMsgSize = 16 * 1024 * 1024;
  std::string msg(kMsgSize, 0x42);
  std::promise<void> readDoneProm;

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
          readDoneProm.set_value();
        });
        readDoneProm.get_future().get();
      });
}

TEST_P(TransportTest, MultiWrite) {
  constexpr int kMsgSize = 16 * 1024;
  constexpr int numMsg = 10;
  std::string msg[numMsg];

  for (int i = 0; i < numMsg; i++) {
    msg[i] = std::string(kMsgSize, static_cast<char>(i));
  }
  std::promise<void> writeDoneProm;
  std::promise<void> readDoneProm;

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        for (int i = 0; i < numMsg; i++) {
          conn->write(
              msg[i].c_str(), msg[i].length(), [conn](const Error& error) {
                ASSERT_FALSE(error) << error.what();
              });
        }
        writeDoneProm.set_value();
      },
      [&](std::shared_ptr<Connection> conn) {
        writeDoneProm.get_future().get();
        for (int i = 0; i < numMsg; i++) {
          // ASSERT_EQ(kMsgSize, msg[i].length());
          conn->read(
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
                  readDoneProm.set_value();
                }
              });
        }
        readDoneProm.get_future().get();
      });
}
