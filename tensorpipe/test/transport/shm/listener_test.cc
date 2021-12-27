/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/shm/shm_test.h>

#include <chrono>
#include <future>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nop/serializer.h>
#include <nop/structure.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

namespace {

class ShmListenerTest : public TransportTest {};

SHMTransportTestHelper helper;

std::string generateUniqueAddr() {
  const ::testing::TestInfo* const testInfo =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::ostringstream ss;
  ss << "tensorpipe_test_" << testInfo->test_suite_name() << "."
     << testInfo->name() << "_" << ::getpid();
  return ss.str();
}

} // namespace

TEST_P(ShmListenerTest, ExplicitAbstractSocketName) {
  std::string expectedAddr = generateUniqueAddr();
  std::shared_ptr<Context> ctx = GetParam()->getContext();
  std::shared_ptr<Listener> listener = ctx->listen(expectedAddr);
  std::string actualAddr = listener->addr();
  ASSERT_EQ(actualAddr, expectedAddr);
  std::shared_ptr<Connection> outgoingConnection = ctx->connect(actualAddr);
  std::promise<void> prom;
  listener->accept(
      [&](const Error& error, std::shared_ptr<Connection> /* unused */) {
        EXPECT_FALSE(error) << error.what();
        prom.set_value();
      });
  std::future_status res = prom.get_future().wait_for(std::chrono::seconds(1));
  ASSERT_NE(res, std::future_status::timeout);
}

TEST_P(ShmListenerTest, AutobindAbstractSocketName) {
  std::shared_ptr<Context> ctx = GetParam()->getContext();
  std::shared_ptr<Listener> listener = ctx->listen("");
  std::string addr = listener->addr();
  ASSERT_NE(addr, "");
  // Since Linux 2.3.15 (Aug 1999) the address is in this format, see unix(7).
  ASSERT_THAT(addr, ::testing::MatchesRegex("[0-9a-f]{5}"));
  std::shared_ptr<Connection> outgoingConnection = ctx->connect(addr);
  std::promise<void> prom;
  listener->accept(
      [&](const Error& error, std::shared_ptr<Connection> /* unused */) {
        EXPECT_FALSE(error) << error.what();
        prom.set_value();
      });
  std::future_status res = prom.get_future().wait_for(std::chrono::seconds(1));
  ASSERT_NE(res, std::future_status::timeout);
}

INSTANTIATE_TEST_CASE_P(Shm, ShmListenerTest, ::testing::Values(&helper));
