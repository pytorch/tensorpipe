/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/shm/context.h>

#include <gtest/gtest.h>

using namespace tensorpipe::transport;

TEST(Context, Basics) {
  auto context = std::make_shared<shm::Context>();
  auto addr = "foobar";

  {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::shared_ptr<Connection>> connections;

    // Listener runs callback for every new connection.
    auto listener = context->listen(addr);
    listener->accept([&](std::shared_ptr<Connection> connection) {
      std::lock_guard<std::mutex> lock(mutex);
      connections.push_back(std::move(connection));
      cv.notify_one();
    });

    // Connect to listener.
    auto conn = context->connect(addr);

    // Wait for new connection
    {
      std::unique_lock<std::mutex> lock(mutex);
      while (connections.empty()) {
        cv.wait(lock);
      }
    }
  }

  context->join();
}

TEST(Context, DomainDescriptor) {
  auto context = std::make_shared<shm::Context>();

  {
    const auto& domainDescriptor = context->domainDescriptor();
    EXPECT_FALSE(domainDescriptor.empty());
  }

  context->join();
}
