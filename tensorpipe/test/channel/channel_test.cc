/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/channel_test.h>

#include <numeric>

#include <tensorpipe/common/queue.h>

using namespace tensorpipe;
using namespace tensorpipe::channel;

TEST_P(ChannelTest, DomainDescriptor) {
  std::shared_ptr<Context> context1 = GetParam()->makeContext("ctx1");
  std::shared_ptr<Context> context2 = GetParam()->makeContext("ctx2");
  EXPECT_FALSE(context1->domainDescriptor().empty());
  EXPECT_FALSE(context2->domainDescriptor().empty());
  EXPECT_EQ(context1->domainDescriptor(), context2->domainDescriptor());
}

TEST_P(ChannelTest, ClientToServer) {
  static constexpr int dataSize = 256;

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("server");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Perform send and wait for completion.
        std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
        std::future<Error> sendFuture;
        std::tie(descriptorFuture, sendFuture) =
            sendWithFuture(channel, data.data(), data.size());
        Error descriptorError;
        Channel::TDescriptor descriptor;
        std::tie(descriptorError, descriptor) = descriptorFuture.get();
        EXPECT_FALSE(descriptorError) << descriptorError.what();
        peers_->send(PeerGroup::kClient, descriptor);
        Error sendError = sendFuture.get();
        EXPECT_FALSE(sendError) << sendError.what();

        peers_->done(PeerGroup::kServer);
        peers_->join(PeerGroup::kServer);

        ctx->join();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("client");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with zeroes.
        std::vector<uint8_t> data(dataSize);
        std::fill(data.begin(), data.end(), 0);

        // Perform recv and wait for completion.
        auto descriptor = peers_->recv(PeerGroup::kClient);
        std::future<Error> recvFuture =
            recvWithFuture(channel, descriptor, data.data(), dataSize);
        Error recvError = recvFuture.get();
        EXPECT_FALSE(recvError) << recvError.what();

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }

        peers_->done(PeerGroup::kClient);
        peers_->join(PeerGroup::kClient);

        ctx->join();
      });
};

TEST_P(ChannelTest, ServerToClient) {
  static constexpr int dataSize = 256;

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("server");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kListen);

        // Initialize with zeroes.
        std::vector<uint8_t> data(dataSize);
        std::fill(data.begin(), data.end(), 0);

        // Perform recv and wait for completion.
        auto descriptor = peers_->recv(PeerGroup::kServer);
        std::future<Error> recvFuture =
            recvWithFuture(channel, descriptor, data.data(), data.size());
        Error recvError = recvFuture.get();
        EXPECT_FALSE(recvError) << recvError.what();

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }

        peers_->done(PeerGroup::kServer);
        peers_->join(PeerGroup::kServer);

        ctx->join();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("client");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Perform send and wait for completion.
        std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
        std::future<Error> sendFuture;
        std::tie(descriptorFuture, sendFuture) =
            sendWithFuture(channel, data.data(), data.size());
        Error descriptorError;
        Channel::TDescriptor descriptor;
        std::tie(descriptorError, descriptor) = descriptorFuture.get();
        EXPECT_FALSE(descriptorError) << descriptorError.what();
        peers_->send(PeerGroup::kServer, descriptor);
        Error sendError = sendFuture.get();
        EXPECT_FALSE(sendError) << sendError.what();

        peers_->done(PeerGroup::kClient);
        peers_->join(PeerGroup::kClient);

        ctx->join();
      });
};

TEST_P(ChannelTest, SendMultipleTensors) {
  constexpr auto dataSize = 256 * 1024; // 256KB
  constexpr int numTensors = 100;

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("server");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Error futures
        std::vector<std::future<Error>> sendFutures;

        // Perform send and wait for completion.
        for (int i = 0; i < numTensors; i++) {
          std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
          std::future<Error> sendFuture;
          std::tie(descriptorFuture, sendFuture) =
              sendWithFuture(channel, data.data(), data.size());
          Error descriptorError;
          Channel::TDescriptor descriptor;
          std::tie(descriptorError, descriptor) = descriptorFuture.get();
          EXPECT_FALSE(descriptorError) << descriptorError.what();
          peers_->send(PeerGroup::kClient, descriptor);
          sendFutures.push_back(std::move(sendFuture));
        }
        for (auto& sendFuture : sendFutures) {
          Error sendError = sendFuture.get();
          EXPECT_FALSE(sendError) << sendError.what();
        }

        peers_->done(PeerGroup::kServer);
        peers_->join(PeerGroup::kServer);

        ctx->join();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("client");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with zeroes.
        std::vector<std::vector<uint8_t>> dataVec(
            numTensors, std::vector<uint8_t>(dataSize, 0));

        // Error futures
        std::vector<std::future<Error>> recvFutures;

        // Perform recv and wait for completion.
        for (int i = 0; i < numTensors; i++) {
          auto descriptor = peers_->recv(PeerGroup::kClient);
          std::future<Error> recvFuture =
              recvWithFuture(channel, descriptor, dataVec[i].data(), dataSize);
          recvFutures.push_back(std::move(recvFuture));
        }
        for (auto& recvFuture : recvFutures) {
          Error recvError = recvFuture.get();
          EXPECT_FALSE(recvError) << recvError.what();
        }

        // Validate contents of vector.
        for (auto& data : dataVec) {
          for (int i = 0; i < data.size(); i++) {
            uint8_t value = i;
            EXPECT_EQ(data[i], value);
          }
        }

        peers_->done(PeerGroup::kClient);
        peers_->join(PeerGroup::kClient);

        ctx->join();
      });
}

TEST_P(ChannelTest, NullPointer) {
  // Call send and recv with a null pointer and a length of 0.

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("server");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kListen);

        // Perform send and wait for completion.
        std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
        std::future<Error> sendFuture;
        std::tie(descriptorFuture, sendFuture) =
            sendWithFuture(channel, nullptr, 0);
        Error descriptorError;
        Channel::TDescriptor descriptor;
        std::tie(descriptorError, descriptor) = descriptorFuture.get();
        EXPECT_FALSE(descriptorError) << descriptorError.what();
        peers_->send(PeerGroup::kClient, descriptor);
        Error sendError = sendFuture.get();
        EXPECT_FALSE(sendError) << sendError.what();

        peers_->done(PeerGroup::kServer);
        peers_->join(PeerGroup::kServer);

        ctx->join();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("client");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kConnect);

        // Perform recv and wait for completion.
        auto descriptor = peers_->recv(PeerGroup::kClient);
        std::future<Error> recvFuture =
            recvWithFuture(channel, descriptor, nullptr, 0);
        Error recvError = recvFuture.get();
        EXPECT_FALSE(recvError) << recvError.what();

        peers_->done(PeerGroup::kClient);
        peers_->join(PeerGroup::kClient);

        ctx->join();
      });
}

TEST_P(ChannelTest, EmptyTensor) {
  // Call send and recv with a length of 0 but a non-null pointer.

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("server");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kListen);

        // Allocate a non-empty vector so that its .data() pointer is non-null.
        std::vector<uint8_t> data(1);

        // Perform send and wait for completion.
        std::future<std::tuple<Error, Channel::TDescriptor>> descriptorFuture;
        std::future<Error> sendFuture;
        std::tie(descriptorFuture, sendFuture) =
            sendWithFuture(channel, data.data(), 0);
        Error descriptorError;
        Channel::TDescriptor descriptor;
        std::tie(descriptorError, descriptor) = descriptorFuture.get();
        EXPECT_FALSE(descriptorError) << descriptorError.what();
        peers_->send(PeerGroup::kClient, descriptor);
        Error sendError = sendFuture.get();
        EXPECT_FALSE(sendError) << sendError.what();

        peers_->done(PeerGroup::kServer);
        peers_->join(PeerGroup::kServer);

        ctx->join();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("client");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kConnect);

        // Allocate a non-empty vector so that its .data() pointer is non-null.
        std::vector<uint8_t> data(1);

        // Perform recv and wait for completion.
        auto descriptor = peers_->recv(PeerGroup::kClient);
        std::future<Error> recvFuture =
            recvWithFuture(channel, descriptor, data.data(), 0);
        Error recvError = recvFuture.get();
        EXPECT_FALSE(recvError) << recvError.what();

        peers_->done(PeerGroup::kClient);
        peers_->join(PeerGroup::kClient);

        ctx->join();
      });
}

TEST_P(ChannelTest, contextIsNotJoined) {
  const std::string kReady = "ready";

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> context = GetParam()->makeContext("server");
        peers_->send(PeerGroup::kClient, kReady);
        context->createChannel(std::move(conn), Channel::Endpoint::kListen);
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> context = GetParam()->makeContext("client");
        EXPECT_EQ(kReady, peers_->recv(PeerGroup::kClient));
        context->createChannel(std::move(conn), Channel::Endpoint::kConnect);
      });
}

TEST_P(ChannelTest, CallbacksAreDeferred) {
  // This test wants to make sure that the "heavy lifting" of copying data isn't
  // performed inline inside the recv method as that would make the user-facing
  // read method of the pipe blocking.
  // However, since we can't really check that behavior, we'll check a highly
  // correlated one: that the recv callback isn't called inline from within the
  // recv method. We do so by having that behavior cause a deadlock.
  constexpr auto dataSize = 256;

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("server");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kListen);

        // Initialize with sequential values.
        std::vector<uint8_t> data(dataSize);
        std::iota(data.begin(), data.end(), 0);

        // Perform send and wait for completion.
        std::promise<std::tuple<Error, Channel::TDescriptor>> descriptorPromise;
        std::promise<Error> sendPromise;
        std::mutex mutex;
        std::unique_lock<std::mutex> callerLock(mutex);
        channel->send(
            data.data(),
            data.size(),
            [&descriptorPromise](
                const Error& error, Channel::TDescriptor descriptor) {
              descriptorPromise.set_value(
                  std::make_tuple(error, std::move(descriptor)));
            },
            [&sendPromise, &mutex](const Error& error) {
              std::unique_lock<std::mutex> calleeLock(mutex);
              sendPromise.set_value(error);
            });
        callerLock.unlock();
        Error descriptorError;
        Channel::TDescriptor descriptor;
        std::tie(descriptorError, descriptor) =
            descriptorPromise.get_future().get();
        EXPECT_FALSE(descriptorError) << descriptorError.what();
        peers_->send(PeerGroup::kClient, descriptor);
        Error sendError = sendPromise.get_future().get();
        EXPECT_FALSE(sendError) << sendError.what();

        peers_->done(PeerGroup::kServer);
        peers_->join(PeerGroup::kServer);

        ctx->join();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("client");
        auto channel =
            ctx->createChannel(std::move(conn), Channel::Endpoint::kConnect);

        // Initialize with zeroes.
        std::vector<uint8_t> data(dataSize);
        std::fill(data.begin(), data.end(), 0);

        // Perform recv and wait for completion.
        std::promise<Error> recvPromise;
        std::mutex mutex;
        std::unique_lock<std::mutex> callerLock(mutex);
        auto descriptor = peers_->recv(PeerGroup::kClient);
        channel->recv(
            descriptor,
            data.data(),
            data.size(),
            [&recvPromise, &mutex](const Error& error) {
              std::unique_lock<std::mutex> calleeLock(mutex);
              recvPromise.set_value(error);
            });
        callerLock.unlock();
        Error recvError = recvPromise.get_future().get();
        EXPECT_FALSE(recvError) << recvError.what();

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }

        peers_->done(PeerGroup::kClient);
        peers_->join(PeerGroup::kClient);

        ctx->join();
      });
}
