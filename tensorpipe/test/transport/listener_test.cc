/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/transport_test.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

TEST_P(TransportTest, Listener_Basics) {
  auto context = GetParam()->getContext();
  auto addr = GetParam()->defaultAddr();

  {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::shared_ptr<Connection>> connections;

    // Listener runs callback for every new connection.
    auto listener = context->listen(addr);
    listener->accept(
        [&](const Error& error, std::shared_ptr<Connection> connection) {
          ASSERT_FALSE(error) << error.what();
          std::lock_guard<std::mutex> lock(mutex);
          connections.push_back(std::move(connection));
          cv.notify_one();
        });

    // Connect to listener.
    auto connection = context->connect(listener->addr());

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

TEST_P(TransportTest, Listener_AcceptCallbacksAreQueued) {
  auto context = GetParam()->getContext();
  auto addr = GetParam()->defaultAddr();

  {
    auto listener = context->listen(addr);
    int numAccepts = 0;
    std::promise<void> donePromise;
    for (int i = 0; i < 10; i += 1) {
      listener->accept([&, i](const Error& error, std::shared_ptr<Connection>) {
        if (error) {
          donePromise.set_exception(
              std::make_exception_ptr(std::runtime_error(error.what())));
        } else {
          EXPECT_EQ(i, numAccepts);
          numAccepts++;
          if (numAccepts == 10) {
            donePromise.set_value();
          }
        }
      });
    }
    for (int i = 0; i < 10; i += 1) {
      context->connect(listener->addr());
    }

    donePromise.get_future().get();
  }

  context->join();
}

TEST_P(TransportTest, Listener_IncomingConnectionsAreQueued) {
  auto context = GetParam()->getContext();
  auto addr = GetParam()->defaultAddr();

  {
    auto listener = context->listen(addr);
    int numAccepts = 0;
    std::promise<void> donePromise;
    for (int i = 0; i < 10; i += 1) {
      context->connect(listener->addr());
    }
    for (int i = 0; i < 10; i += 1) {
      listener->accept([&, i](const Error& error, std::shared_ptr<Connection>) {
        if (error) {
          donePromise.set_exception(
              std::make_exception_ptr(std::runtime_error(error.what())));
        } else {
          EXPECT_EQ(i, numAccepts);
          numAccepts++;
          if (numAccepts == 10) {
            donePromise.set_value();
          }
        }
      });
    }

    donePromise.get_future().get();
  }

  context->join();
}
