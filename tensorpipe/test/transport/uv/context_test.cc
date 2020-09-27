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

class UVTransportContextTest : public TransportTest {};

UVTransportTestHelper helper;

} // namespace

using namespace tensorpipe;

// Linux-only because OSX machines on CircleCI cannot resolve their hostname
#ifdef __linux__
TEST_P(UVTransportContextTest, LookupHostnameAddress) {
  auto context = std::dynamic_pointer_cast<transport::uv::Context>(
      GetParam()->getContext());
  ASSERT_TRUE(context);

  Error error;
  std::string addr;
  std::tie(error, addr) = context->lookupAddrForHostname();
  EXPECT_FALSE(error) << error.what();
  EXPECT_NE(addr, "");
}
#endif

// Linux-only because OSX uses "lo0" for the loopback interface
#ifdef __linux__
TEST_P(UVTransportContextTest, LookupInterfaceAddress) {
  auto context = std::dynamic_pointer_cast<transport::uv::Context>(
      GetParam()->getContext());
  ASSERT_TRUE(context);

  Error error;
  std::string addr;
  std::tie(error, addr) = context->lookupAddrForIface("lo");
  EXPECT_FALSE(error) << error.what();
  EXPECT_NE(addr, "");
}
#endif

INSTANTIATE_TEST_CASE_P(Uv, UVTransportContextTest, ::testing::Values(&helper));
