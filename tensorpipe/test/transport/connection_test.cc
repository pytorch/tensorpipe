/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/connection_test.h>
#include <tensorpipe/test/transport/shm/connection_test.h>
#include <tensorpipe/test/transport/uv/connection_test.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

using ConnectionTypes =
    ::testing::Types<SHMConnectionTestHelper, UVConnectionTestHelper>;

// NOTE: When upgrading googletest, the following will allow nicer reported test
// names.
//
// TYPED_TEST_SUITE(ConnectionTest, ConnectionTypes, ConnectionTypeNames);
TYPED_TEST_CASE(ConnectionTest, ConnectionTypes);

TYPED_TEST(ConnectionTest, Initialization) {
  constexpr size_t numBytes = 13;
  std::array<char, numBytes> garbage;

  this->test_connection(
      [&](std::shared_ptr<Connection> conn) {
        conn->read(
            [&, conn](
                const Error& error, const void* /* unused */, size_t len) {
              ASSERT_FALSE(error) << error.what();
              ASSERT_EQ(len, garbage.size());
            });
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->write(
            garbage.data(), garbage.size(), [&, conn](const Error& error) {
              ASSERT_FALSE(error) << error.what();
            });
      });
}

TYPED_TEST(ConnectionTest, InitializationError) {
  this->test_connection(
      [&](std::shared_ptr<Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        conn->read([conn](
                       const Error& error,
                       const void* /* unused */,
                       size_t /* unused */) { ASSERT_TRUE(error); });
      });
}

TYPED_TEST(ConnectionTest, DestroyConnectionFromCallback) {
  this->test_connection(
      [&](std::shared_ptr<Connection> /* unused */) {
        // Closes connection
      },
      [&](std::shared_ptr<Connection> conn) {
        // This should be the only connection instance.
        EXPECT_EQ(conn.use_count(), 1);
        // Move connection instance to lambda scope, so we can destroy
        // the only instance we have from the callback itself. This
        // tests that the transport keeps the connection alive as long
        // as it's executing a callback.
        conn->read([conn](
                       const Error& /* unused */,
                       const void* /* unused */,
                       size_t /* unused */) mutable {
          // Destroy connection from within callback.
          EXPECT_GT(conn.use_count(), 1);
          conn.reset();
        });
      });
}

TYPED_TEST(ConnectionTest, AcceptCallbacksAreQueued) {
  TypeParam helper;
  auto listener = helper.getListener();
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
    helper.connect(listener->addr());
  }
  donePromise.get_future().get();
}

TYPED_TEST(ConnectionTest, IncomingConnectionsAreQueued) {
  TypeParam helper;
  auto listener = helper.getListener();
  int numAccepts = 0;
  std::promise<void> donePromise;
  for (int i = 0; i < 10; i += 1) {
    helper.connect(listener->addr());
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
