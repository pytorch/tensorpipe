/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/uv/uv_test.h>

#include <gtest/gtest.h>

namespace {

class UVTransportConnectionTest : public TransportTest {};

UVTransportTestHelper helper;

} // namespace

using namespace tensorpipe;
using namespace tensorpipe::transport;

TEST_P(UVTransportConnectionTest, LargeWrite) {
  constexpr int kMsgSize = 16 * 1024 * 1024;
  std::string msg(kMsgSize, 0x42);
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  std::future<void> writeCompletedFuture = writeCompletedProm.get_future();
  std::future<void> readCompletedFuture = readCompletedProm.get_future();

  this->testConnection(
      [&](std::shared_ptr<Connection> conn) {
        this->doWrite(
            conn, msg.c_str(), msg.length(), [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              writeCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      },
      [&](std::shared_ptr<Connection> conn) {
        this->doRead(
            conn, [&, conn](const Error& error, const void* data, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, msg.length());
              const char* cdata = (const char*)data;
              for (int i = 0; i < len; ++i) {
                const char c = cdata[i];
                ASSERT_EQ(c, msg[i]) << "Wrong value at position " << i
                                     << " of " << msg.length();
              }
              readCompletedProm.set_value();
            });
        writeCompletedFuture.wait();
        readCompletedFuture.wait();
      });
}

INSTANTIATE_TEST_CASE_P(
    Uv,
    UVTransportConnectionTest,
    ::testing::Values(&helper));
