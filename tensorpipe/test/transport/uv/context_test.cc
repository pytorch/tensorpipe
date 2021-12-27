/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/uv/uv_test.h>
#include <tensorpipe/transport/uv/utility.h>

#include <gtest/gtest.h>

namespace {

class UVTransportContextTest : public TransportTest {};

UVTransportTestHelper helper;

} // namespace

using namespace tensorpipe;

// Linux-only because OSX machines on CircleCI cannot resolve their hostname
#ifdef __linux__
TEST_P(UVTransportContextTest, LookupHostnameAddress) {
  Error error;
  std::string addr;
  std::tie(error, addr) = transport::uv::lookupAddrForHostname();
  EXPECT_FALSE(error) << error.what();
  EXPECT_NE(addr, "");
}
#endif

// Interface name conventions change based on platform. Linux uses "lo", OSX
// uses lo0, Windows uses integers.
#ifdef __linux__
#define LOOPBACK_INTERFACE "lo"
#elif __APPLE__
#define LOOPBACK_INTERFACE "lo0"
#endif

#ifdef LOOPBACK_INTERFACE
TEST_P(UVTransportContextTest, LookupInterfaceAddress) {
  Error error;
  std::string addr;
  std::tie(error, addr) = transport::uv::lookupAddrForIface(LOOPBACK_INTERFACE);
  EXPECT_FALSE(error) << error.what();
  EXPECT_NE(addr, "");
}
#endif

TEST_P(UVTransportContextTest, LookupAddressLikeNccl) {
  Error error;
  std::string addr;
  std::tie(error, addr) = transport::uv::lookupAddrLikeNccl();
  EXPECT_FALSE(error) << error.what();
  EXPECT_NE(addr, "");
}

INSTANTIATE_TEST_CASE_P(Uv, UVTransportContextTest, ::testing::Values(&helper));
