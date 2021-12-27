/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/system.h>

#include <gtest/gtest.h>

using namespace tensorpipe;

TEST(Pow2, isPow2) {
  for (uint64_t i = 0; i < 63; ++i) {
    EXPECT_TRUE(isPow2(1ull << i));
  }

  EXPECT_FALSE(isPow2(3));
  EXPECT_FALSE(isPow2(5));
  EXPECT_FALSE(isPow2(10));
  EXPECT_FALSE(isPow2(15));
  EXPECT_TRUE(isPow2(16));
  EXPECT_FALSE(isPow2(17));
  EXPECT_FALSE(isPow2(18));
  EXPECT_FALSE(isPow2(25));
  EXPECT_FALSE(isPow2(1028));
}

TEST(Pow2, nextPow2) {
  for (uint64_t i = 0; i < 63; ++i) {
    uint64_t p2 = 1ull << i;
    uint64_t nextP2 = 1ull << (i + 1);
    EXPECT_EQ(nextPow2(p2), p2);
    EXPECT_EQ(nextPow2(p2 + 1), nextP2);
  }
}
