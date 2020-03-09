/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/socket.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

TEST(Sockaddr, FromToString) {
  auto addr = shm::Sockaddr::createAbstractUnixAddr("foo");
  ASSERT_EQ(addr.str(), std::string("foo"));
}
