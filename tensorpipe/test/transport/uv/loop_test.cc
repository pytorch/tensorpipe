/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <tensorpipe/transport/uv/loop.h>

using namespace tensorpipe::transport::uv;

namespace test {
namespace transport {
namespace uv {

TEST(UvLoop, Defer) {
  Loop loop;

  {
    // Defer function on event loop thread.
    std::promise<std::thread::id> prom;
    loop.deferToLoop([&] { prom.set_value(std::this_thread::get_id()); });
    ASSERT_NE(std::this_thread::get_id(), prom.get_future().get());
  }

  loop.join();
}

} // namespace uv
} // namespace transport
} // namespace test
