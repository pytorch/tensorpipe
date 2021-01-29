/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/cpu_context.h>
#include <tensorpipe/test/channel/channel_test.h>

using namespace tensorpipe;
using namespace tensorpipe::channel;

// Call send and recv with a null pointer and a length of 0.
class NullPointerTest : public ClientServerChannelTestCase<CpuBuffer> {
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CpuContext> ctx = this->helper_->makeContext("server");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kListen);

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) =
        sendWithFuture(channel, CpuBuffer{nullptr, 0});
    Error descriptorError;
    TDescriptor descriptor;
    std::tie(descriptorError, descriptor) = descriptorFuture.get();
    EXPECT_FALSE(descriptorError) << descriptorError.what();
    this->peers_->send(PeerGroup::kClient, descriptor);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CpuContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kConnect);

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    std::future<Error> recvFuture =
        recvWithFuture(channel, descriptor, CpuBuffer{nullptr, 0});
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CpuChannelTestSuite, NullPointer);

// Call send and recv with a length of 0 but a non-null pointer.
class EmptyTensorTest : public ClientServerChannelTestCase<CpuBuffer> {
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<CpuBuffer>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kListen);

    // Allocate a non-empty vector so that its .data() pointer is non-null.
    std::vector<uint8_t> data(1);
    DataWrapper<CpuBuffer> wrappedData(data);
    CpuBuffer buffer = wrappedData.buffer();
    buffer.length = 0;

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) = sendWithFuture(channel, buffer);
    Error descriptorError;
    TDescriptor descriptor;
    std::tie(descriptorError, descriptor) = descriptorFuture.get();
    EXPECT_FALSE(descriptorError) << descriptorError.what();
    this->peers_->send(PeerGroup::kClient, descriptor);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CpuContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kConnect);

    // Allocate a non-empty vector so that its .data() pointer is non-null.
    DataWrapper<CpuBuffer> wrappedData(1);
    CpuBuffer buffer = wrappedData.buffer();
    buffer.length = 0;

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    std::future<Error> recvFuture = recvWithFuture(channel, descriptor, buffer);
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CpuChannelTestSuite, EmptyTensor);

// This test wants to make sure that the "heavy lifting" of copying data isn't
// performed inline inside the recv method as that would make the user-facing
// read method of the pipe blocking.
// However, since we can't really check that behavior, we'll check a highly
// correlated one: that the recv callback isn't called inline from within the
// recv method. We do so by having that behavior cause a deadlock.
class CallbacksAreDeferredTest : public ClientServerChannelTestCase<CpuBuffer> {
  static constexpr auto kDataSize = 256;

 public:
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CpuContext> ctx = this->helper_->makeContext("server");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kListen);

    // Initialize with sequential values.
    std::vector<uint8_t> data(kDataSize);
    std::iota(data.begin(), data.end(), 0);

    // Perform send and wait for completion.
    std::promise<std::tuple<Error, TDescriptor>> descriptorPromise;
    std::promise<Error> sendPromise;
    std::mutex mutex;
    std::unique_lock<std::mutex> callerLock(mutex);
    channel->send(
        CpuBuffer{data.data(), kDataSize},
        [&descriptorPromise](const Error& error, TDescriptor descriptor) {
          descriptorPromise.set_value(
              std::make_tuple(error, std::move(descriptor)));
        },
        [&sendPromise, &mutex](const Error& error) {
          std::unique_lock<std::mutex> calleeLock(mutex);
          sendPromise.set_value(error);
        });
    callerLock.unlock();
    Error descriptorError;
    TDescriptor descriptor;
    std::tie(descriptorError, descriptor) =
        descriptorPromise.get_future().get();
    EXPECT_FALSE(descriptorError) << descriptorError.what();
    this->peers_->send(PeerGroup::kClient, descriptor);
    Error sendError = sendPromise.get_future().get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CpuContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kConnect);

    // Initialize with zeroes.
    std::vector<uint8_t> data(kDataSize);
    std::fill(data.begin(), data.end(), 0);

    // Perform recv and wait for completion.
    std::promise<Error> recvPromise;
    std::mutex mutex;
    std::unique_lock<std::mutex> callerLock(mutex);
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    channel->recv(
        descriptor,
        CpuBuffer{data.data(), kDataSize},
        [&recvPromise, &mutex](const Error& error) {
          std::unique_lock<std::mutex> calleeLock(mutex);
          recvPromise.set_value(error);
        });
    callerLock.unlock();
    Error recvError = recvPromise.get_future().get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    for (auto i = 0; i < kDataSize; i++) {
      EXPECT_EQ(data[i], i);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CpuChannelTestSuite, CallbacksAreDeferred);
