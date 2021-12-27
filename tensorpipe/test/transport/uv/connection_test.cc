/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        doWrite(conn, msg.c_str(), msg.length(), [&, conn](const Error& error) {
          ASSERT_FALSE(error) << error.what();
          peers_->done(PeerGroup::kServer);
        });
        peers_->join(PeerGroup::kServer);
      },
      [&](std::shared_ptr<Connection> conn) {
        doRead(
            conn, [&, conn](const Error& error, const void* data, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, msg.length());
              const char* cdata = (const char*)data;
              for (int i = 0; i < len; ++i) {
                const char c = cdata[i];
                ASSERT_EQ(c, msg[i]) << "Wrong value at position " << i
                                     << " of " << msg.length();
              }
              peers_->done(PeerGroup::kClient);
            });
        peers_->join(PeerGroup::kClient);
      });
}

INSTANTIATE_TEST_CASE_P(
    Uv,
    UVTransportConnectionTest,
    ::testing::Values(&helper));
