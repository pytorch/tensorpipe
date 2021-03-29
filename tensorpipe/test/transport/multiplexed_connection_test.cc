/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/transport_test.h>

#include <array>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/transport/multiplexed_connection.h>
#include <tensorpipe/common/deferred_executor.h>

using namespace tensorpipe;
using namespace tensorpipe::transport;

namespace {

struct Foo {
  int32_t i;
  int64_t j;
  NOP_STRUCTURE(Foo, i, j);
};

struct Bar {
  std::string a;
  NOP_STRUCTURE(Bar, a);
};

struct Baz {
  NOP_STRUCTURE(Baz);
};

template <typename TConn, typename T>
std::future<tensorpipe::Error> writeWithFuture(TConn& conn, const T& value) {
  auto prom = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = prom->get_future();
  conn.write(value, [prom{std::move(prom)}](const tensorpipe::Error& error) {
    prom->set_value(error);
  });

  return std::move(future);
}

template <typename TConn, typename T>
std::future<tensorpipe::Error> readWithFuture(TConn& conn, T& value) {
  auto prom = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = prom->get_future();
  conn.read(value, [prom{std::move(prom)}](const tensorpipe::Error& error) {
    prom->set_value(error);
  });

  return std::move(future);
}

} // namespace

TEST_P(TransportTest, MultiplexedConnection) {
  testConnection(
      [&](std::shared_ptr<Connection> conn) {
        OnDemandDeferredExecutor executor;
        {
          MultiplexedConnection<Foo, Bar, Baz> mconn(conn, executor);

          auto future1 = writeWithFuture(mconn, Foo{42, 1337});
          EXPECT_EQ(future1.get(), Error::kSuccess);

          auto future2 = writeWithFuture(mconn, Bar{"hello"});
          EXPECT_EQ(future2.get(), Error::kSuccess);

          auto future3 = writeWithFuture(mconn, Baz{});
          EXPECT_EQ(future3.get(), Error::kSuccess);

          peers_->done(PeerGroup::kServer);
          peers_->join(PeerGroup::kServer);
        }
      },
      [&](std::shared_ptr<Connection> conn) {
        OnDemandDeferredExecutor executor;
        {
          MultiplexedConnection<Foo, Bar, Baz> mconn(conn, executor);

          Baz baz;
          auto future1 = readWithFuture(mconn, baz);
          EXPECT_EQ(future1.get(), Error::kSuccess);

          Foo foo;
          auto future2 = readWithFuture(mconn, foo);
          EXPECT_EQ(future2.get(), Error::kSuccess);
          EXPECT_EQ(foo.i, 42);
          EXPECT_EQ(foo.j, 1337);

          Bar bar;
          auto future3 = readWithFuture(mconn, bar);
          EXPECT_EQ(future3.get(), Error::kSuccess);
          EXPECT_EQ(bar.a, "hello");

          peers_->done(PeerGroup::kClient);
          peers_->join(PeerGroup::kClient);
        }
      });
}
