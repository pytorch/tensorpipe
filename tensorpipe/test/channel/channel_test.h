/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/buffer.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/test/peer_group.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>
#include <tensorpipe/transport/uv/factory.h>

class DataWrapper {
 public:
  virtual tensorpipe::Buffer buffer() const = 0;

  virtual size_t bufferLength() const = 0;

  virtual std::vector<uint8_t> unwrap() = 0;

  virtual ~DataWrapper() = default;
};

class ChannelTestHelper {
 public:
  virtual ~ChannelTestHelper() = default;

  std::shared_ptr<tensorpipe::channel::Context> makeContext(
      std::string id,
      bool skipViabilityCheck = false) {
    std::shared_ptr<tensorpipe::channel::Context> ctx =
        makeContextInternal(std::move(id));
    if (!skipViabilityCheck) {
      EXPECT_TRUE(ctx->isViable());
    }
    return ctx;
  }

  virtual std::shared_ptr<PeerGroup> makePeerGroup() {
    return std::make_shared<ThreadPeerGroup>();
  }

  virtual std::unique_ptr<DataWrapper> makeDataWrapper(size_t length) = 0;
  virtual std::unique_ptr<DataWrapper> makeDataWrapper(
      std::vector<uint8_t> v) = 0;

 protected:
  virtual std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) = 0;
};

[[nodiscard]] inline std::future<tensorpipe::Error> sendWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel> channel,
    tensorpipe::Buffer buffer,
    size_t length) {
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = promise->get_future();

  channel->send(
      buffer,
      length,
      [promise{std::move(promise)}](const tensorpipe::Error& error) {
        promise->set_value(error);
      });
  return future;
}

[[nodiscard]] inline std::future<tensorpipe::Error> sendWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel> channel,
    const DataWrapper& dataWrapper) {
  return sendWithFuture(
      std::move(channel), dataWrapper.buffer(), dataWrapper.bufferLength());
}

[[nodiscard]] inline std::future<tensorpipe::Error> recvWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel> channel,
    tensorpipe::Buffer buffer,
    size_t length) {
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = promise->get_future();

  channel->recv(
      buffer,
      length,
      [promise{std::move(promise)}](const tensorpipe::Error& error) {
        promise->set_value(error);
      });
  return future;
}

[[nodiscard]] inline std::future<tensorpipe::Error> recvWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel> channel,
    const DataWrapper& dataWrapper) {
  return recvWithFuture(
      std::move(channel), dataWrapper.buffer(), dataWrapper.bufferLength());
}

class ChannelTestCase {
 public:
  virtual void run(ChannelTestHelper* helper) = 0;

  virtual ~ChannelTestCase() = default;
};

class ClientServerChannelTestCase : public ChannelTestCase {
  using MultiAcceptResult = std::pair<
      tensorpipe::Error,
      std::vector<std::shared_ptr<tensorpipe::transport::Connection>>>;

  class MultiAcceptResultPromise {
   public:
    explicit MultiAcceptResultPromise(size_t numConnections)
        : connections_(numConnections) {}

    ~MultiAcceptResultPromise() {
      // Sanity check
      if (!error_) {
        for (const auto& conn : connections_) {
          EXPECT_NE(conn, nullptr);
        }
      }
      promise_.set_value(
          MultiAcceptResult(std::move(error_), std::move(connections_)));
    }

    std::future<MultiAcceptResult> getFuture() {
      return promise_.get_future();
    }

    void setConnection(
        size_t connId,
        std::shared_ptr<tensorpipe::transport::Connection> connection) {
      EXPECT_LT(connId, connections_.size());
      connections_[connId] = std::move(connection);
    }

    void setError(tensorpipe::Error error) {
      std::unique_lock<std::mutex> lock(errorMutex_);
      if (error_) {
        return;
      }
      error_ = std::move(error);
    }

   private:
    tensorpipe::Error error_{tensorpipe::Error::kSuccess};
    std::mutex errorMutex_;
    std::vector<std::shared_ptr<tensorpipe::transport::Connection>>
        connections_;
    std::promise<MultiAcceptResult> promise_;
  };

  std::future<MultiAcceptResult> accept(
      tensorpipe::transport::Listener& listener,
      size_t numConnections) {
    auto promise = std::make_shared<MultiAcceptResultPromise>(numConnections);
    for (size_t i = 0; i < numConnections; ++i) {
      listener.accept(
          [promise](
              const tensorpipe::Error& error,
              std::shared_ptr<tensorpipe::transport::Connection> connection) {
            if (error) {
              promise->setError(std::move(error));
              return;
            }

            connection->read([promise, connection](
                                 const tensorpipe::Error& error,
                                 const void* connIdBuf,
                                 size_t length) mutable {
              if (error) {
                promise->setError(std::move(error));
                return;
              }
              ASSERT_EQ(sizeof(uint64_t), length);
              uint64_t connId = *static_cast<const uint64_t*>(connIdBuf);
              promise->setConnection(connId, std::move(connection));
            });
          });
    }

    return promise->getFuture();
  }

  std::vector<std::shared_ptr<tensorpipe::transport::Connection>> connect(
      std::shared_ptr<tensorpipe::transport::Context> transportCtx,
      std::string addr,
      size_t numConnections) {
    std::vector<std::shared_ptr<tensorpipe::transport::Connection>> connections(
        numConnections);
    for (size_t connId = 0; connId < numConnections; ++connId) {
      connections[connId] = transportCtx->connect(addr);
      auto connIdBuf = std::make_shared<uint64_t>(connId);
      connections[connId]->write(
          connIdBuf.get(),
          sizeof(uint64_t),
          [connIdBuf](const tensorpipe::Error& error) {
            EXPECT_FALSE(error) << error.what();
          });
    }

    return connections;
  }

 public:
  void run(ChannelTestHelper* helper) override {
    auto addr = "127.0.0.1";

    helper_ = helper;
    peers_ = helper_->makePeerGroup();
    peers_->spawn(
        [&] {
          auto transportCtx = tensorpipe::transport::uv::create();
          transportCtx->setId("server_harness");
          auto ctx = helper_->makeContext("server");

          auto listener = transportCtx->listen(addr);

          auto connectionsFuture =
              accept(*listener, ctx->numConnectionsNeeded());
          peers_->send(PeerGroup::kClient, listener->addr());

          tensorpipe::Error connectionsError;
          std::vector<std::shared_ptr<tensorpipe::transport::Connection>>
              connections;
          std::tie(connectionsError, connections) = connectionsFuture.get();
          EXPECT_FALSE(connectionsError) << connectionsError.what();

          auto channel = ctx->createChannel(
              std::move(connections), tensorpipe::channel::Endpoint::kListen);

          server(std::move(channel));

          ctx->join();
          transportCtx->join();

          afterServer();
        },
        [&] {
          auto transportCtx = tensorpipe::transport::uv::create();
          transportCtx->setId("client_harness");
          auto ctx = helper_->makeContext("client");

          auto laddr = peers_->recv(PeerGroup::kClient);

          auto connections =
              connect(transportCtx, laddr, ctx->numConnectionsNeeded());

          auto channel = ctx->createChannel(
              std::move(connections), tensorpipe::channel::Endpoint::kConnect);

          client(std::move(channel));

          ctx->join();
          transportCtx->join();

          afterClient();
        });
  }

  virtual void server(
      std::shared_ptr<tensorpipe::channel::Channel> /* channel */) {}
  virtual void client(
      std::shared_ptr<tensorpipe::channel::Channel> /* channel */) {}

  virtual void afterServer() {}
  virtual void afterClient() {}

 protected:
  ChannelTestHelper* helper_;
  std::shared_ptr<PeerGroup> peers_;
};

class ChannelTestSuite : public ::testing::TestWithParam<ChannelTestHelper*> {};

// Register a channel test.
#define CHANNEL_TEST(suite, name) \
  TEST_P(suite, name) {           \
    name##Test t;                 \
    t.run(GetParam());            \
  }
