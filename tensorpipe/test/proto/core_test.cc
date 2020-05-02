/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/proto/core.pb.h>

#include <gtest/gtest.h>

TEST(Proto, MessageDescriptor) {
  tensorpipe::proto::MessageDescriptor::PayloadDescriptor d;
  d.set_size_in_bytes(10);
  EXPECT_EQ(d.size_in_bytes(), 10);
}
