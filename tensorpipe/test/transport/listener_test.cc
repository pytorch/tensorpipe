/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
    for (int i = 0; i < 10; ++i) {
      listener->accept(
          [&, i](const Error& error, std::shared_ptr<Connection> /*unused*/) {
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

    // Avoid connections to be destroyed before being established.
    std::vector<std::shared_ptr<Connection>> conns;
    for (int i = 0; i < 10; ++i) {
      auto c = context->connect(listener->addr());
      conns.push_back(std::move(c));
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
    // Avoid connections to be destroyed before being established.
    std::vector<std::shared_ptr<Connection>> conns;
    for (int i = 0; i < 10; ++i) {
      auto c = context->connect(listener->addr());
      conns.push_back(std::move(c));
    }
    for (int i = 0; i < 10; ++i) {
      listener->accept(
          [&, i](const Error& error, std::shared_ptr<Connection> /*unused*/) {
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

TEST_P(TransportTest, Listener_CreateThenCloseAndThenGetAddress) {
  auto context = GetParam()->getContext();

  auto listener = context->listen(GetParam()->defaultAddr());
  listener->close();
  auto addr = listener->addr();

  std::promise<void> acceptPromise;
  listener->accept(
      [&](const Error& error, std::shared_ptr<Connection> /*unused*/) {
        if (error) {
          acceptPromise.set_exception(
              std::make_exception_ptr(std::runtime_error(error.what())));
        } else {
          acceptPromise.set_value();
        }
      });

  auto connection = context->connect(addr);
  std::promise<void> writePromise;
  connection->write(nullptr, 0, [&](const Error& error) {
    if (error) {
      writePromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      writePromise.set_value();
    }
  });

  try {
    acceptPromise.get_future().get();
  } catch (const std::runtime_error&) {
    // Expected
  }

  try {
    writePromise.get_future().get();
  } catch (const std::runtime_error&) {
    // Expected
  }

  context->join();
}

TEST_P(TransportTest, Listener_CreateAfterClosingContextAndThenGetAddress) {
  auto context = GetParam()->getContext();

  // This means the listener will be created in an already-closed state.
  context->close();
  auto listener = context->listen(GetParam()->defaultAddr());
  auto addr = listener->addr();

  std::promise<void> acceptPromise;
  listener->accept(
      [&](const Error& error, std::shared_ptr<Connection> /*unused*/) {
        if (error) {
          acceptPromise.set_exception(
              std::make_exception_ptr(std::runtime_error(error.what())));
        } else {
          acceptPromise.set_value();
        }
      });

  auto connection = context->connect(addr);
  std::promise<void> writePromise;
  connection->write(nullptr, 0, [&](const Error& error) {
    if (error) {
      writePromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      writePromise.set_value();
    }
  });

  try {
    acceptPromise.get_future().get();
  } catch (const std::runtime_error&) {
    // Expected
  }

  try {
    writePromise.get_future().get();
  } catch (const std::runtime_error&) {
    // Expected
  }

  context->join();
}
