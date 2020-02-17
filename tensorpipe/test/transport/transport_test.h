/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>
#include <thread>

#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

#include <gtest/gtest.h>

class TransportTestHelper {
 public:
  virtual ~TransportTestHelper() = default;
  virtual std::shared_ptr<tensorpipe::transport::Listener> getListener() = 0;
  virtual std::shared_ptr<tensorpipe::transport::Connection> connect(
      const std::string& addr) = 0;
};

template <class T>
class TransportTest : public ::testing::Test {
 public:
  void test_connection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          listeningFn,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          connectingFn) {
    std::unique_ptr<TransportTestHelper> helper = std::make_unique<T>();

    auto listener = helper->getListener();
    tensorpipe::Queue<std::shared_ptr<tensorpipe::transport::Connection>> queue;
    listener->accept(
        [&](const tensorpipe::Error& error,
            std::shared_ptr<tensorpipe::transport::Connection> conn) {
          ASSERT_FALSE(error) << error.what();
          queue.push(std::move(conn));
        });

    // Start thread for listening side.
    std::thread listeningThread([&]() { listeningFn(queue.pop()); });

    // Capture real listener address.
    const std::string listenerAddr = listener->addr();

    // Start thread for connecting side.
    std::thread connectingThread(
        [&]() { connectingFn(helper->connect(listenerAddr)); });

    // Wait for completion.
    listeningThread.join();
    connectingThread.join();
  }
};

class ConnectionTypeNames {
 public:
  template <class T>
  static std::string GetName(int /* unused */) {
    return T::transportName();
  }
};

// NOTE: When upgrading googletest, the following will allow nicer reported test
// names.
//
// TYPED_TEST_SUITE_P(TransportTest, ConnectionTypeNames);
TYPED_TEST_CASE_P(TransportTest);

TYPED_TEST_P(TransportTest, Initialization) {
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;

  this->test_connection(
      [&](std::shared_ptr<tensorpipe::transport::Connection> conn) {
        conn->read([&, conn](
                       const tensorpipe::Error& error,
                       const void* /* unused */,
                       size_t len) {
          ASSERT_FALSE(error) << error.what();
          ASSERT_EQ(len, garbage.size());
        });
      },
      [&](std::shared_ptr<tensorpipe::transport::Connection> conn) {
        conn->write(
            garbage.data(),
            garbage.size(),
            [&, conn](const tensorpipe::Error& error) {
              ASSERT_FALSE(error) << error.what();
            });
      });
}

TYPED_TEST_P(TransportTest, InitializationError) {
  this->test_connection(
      [&](std::shared_ptr<tensorpipe::transport::Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<tensorpipe::transport::Connection> conn) {
        conn->read([conn](
                       const tensorpipe::Error& error,
                       const void* /* unused */,
                       size_t /* unused */) { ASSERT_TRUE(error); });
      });
}

TYPED_TEST_P(TransportTest, DestroyConnectionFromCallback) {
  this->test_connection(
      [&](std::shared_ptr<tensorpipe::transport::Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<tensorpipe::transport::Connection> conn) {
        // This should be the only connection instance.
        EXPECT_EQ(conn.use_count(), 1);
        // Move connection instance to lambda scope, so we can destroy
        // the only instance we have from the callback itself. This
        // tests that the transport keeps the connection alive as long
        // as it's executing a callback.
        conn->read([conn](
                       const tensorpipe::Error& /* unused */,
                       const void* /* unused */,
                       size_t /* unused */) mutable {
          // Destroy connection from within callback.
          EXPECT_GT(conn.use_count(), 1);
          conn.reset();
        });
      });
}

TYPED_TEST_P(TransportTest, AcceptCallbacksAreQueued) {
  TypeParam helper;
  auto listener = helper.getListener();
  int numAccepts = 0;
  std::promise<void> donePromise;
  for (int i = 0; i < 10; i += 1) {
    listener->accept([&, i](
                         const tensorpipe::Error& error,
                         std::shared_ptr<tensorpipe::transport::Connection>) {
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
    helper.connect(listener->addr());
  }
  donePromise.get_future().get();
}

TYPED_TEST_P(TransportTest, IncomingConnectionsAreQueued) {
  TypeParam helper;
  auto listener = helper.getListener();
  int numAccepts = 0;
  std::promise<void> donePromise;
  for (int i = 0; i < 10; i += 1) {
    helper.connect(listener->addr());
  }
  for (int i = 0; i < 10; i += 1) {
    listener->accept([&, i](
                         const tensorpipe::Error& error,
                         std::shared_ptr<tensorpipe::transport::Connection>) {
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

REGISTER_TYPED_TEST_CASE_P(
    TransportTest,
    Initialization,
    InitializationError,
    DestroyConnectionFromCallback,
    AcceptCallbacksAreQueued,
    IncomingConnectionsAreQueued);
