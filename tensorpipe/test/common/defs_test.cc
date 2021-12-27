/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/defs.h>

#include <gtest/gtest.h>

TEST(Defs, Exception) {
  EXPECT_THROW(TP_THROW_EINVAL(), std::invalid_argument);
  EXPECT_THROW(TP_THROW_EINVAL() << "hola", std::invalid_argument);
  EXPECT_THROW(TP_THROW_EINVAL() << "adioshola", std::invalid_argument);
  EXPECT_THROW(TP_THROW_SYSTEM(ENODATA) << "adioshola", std::system_error);
  EXPECT_THROW(TP_THROW_SYSTEM(EBUSY), std::system_error);
  EXPECT_THROW(TP_THROW_SYSTEM(EBUSY) << "my message", std::system_error);
}
