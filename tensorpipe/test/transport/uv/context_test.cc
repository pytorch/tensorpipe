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

// Disabled because on CircleCI the macOS machines cannot resolve their hostname
TEST_P(UVTransportContextTest, DISABLED_LookupHostnameAddress) {
  auto context = std::dynamic_pointer_cast<transport::uv::Context>(
      GetParam()->getContext());
  ASSERT_TRUE(context);

  Error error;
  std::string addr;
  std::tie(error, addr) = context->lookupAddrForHostname();
  EXPECT_FALSE(error) << error.what();
  EXPECT_NE(addr, "");
}

// Disabled because "lo" isn't a universal convention for the loopback interface
TEST_P(UVTransportContextTest, DISABLED_LookupInterfaceAddress) {
  auto context = std::dynamic_pointer_cast<transport::uv::Context>(
      GetParam()->getContext());
  ASSERT_TRUE(context);

  Error error;
  std::string addr;
  std::tie(error, addr) = context->lookupAddrForIface("lo");
  EXPECT_FALSE(error) << error.what();
  EXPECT_NE(addr, "");
}

INSTANTIATE_TEST_CASE_P(Uv, UVTransportContextTest, ::testing::Values(&helper));
