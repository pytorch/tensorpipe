/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/tensorpipe.h>

#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <string>

#include <gtest/gtest.h>

using namespace tensorpipe;

TEST(Listener, ClosingAbortsOperations) {
  auto context = std::make_shared<Context>();

  context->registerTransport(0, "uv", transport::uv::create());
  context->registerChannel(0, "basic", channel::basic::create());

  {
    auto listener = context->listen({"uv://127.0.0.1"});

    std::promise<void> donePromise;
    listener->accept(
        [&](const Error& error, std::shared_ptr<Pipe> /* unused */) {
          EXPECT_TRUE(error);
          donePromise.set_value();
        });
    listener->close();
    donePromise.get_future().get();
  }

  context->join();
}
