/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/channel_test.h>

#include <numeric>

using namespace tensorpipe;
using namespace tensorpipe::channel;

// Implement this as a client-server test, even we then use just the client,
// because we need this test case to run in a subprocess as in some cases it may
// initialize CUDA and thus would otherwise "pollute" the parent process.
template <typename TBuffer>
class DomainDescriptorTest : public ClientServerChannelTestCase<TBuffer> {
 public:
  void server(std::shared_ptr<transport::Connection> /* unused */) override {
    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<transport::Connection> /* unused */) override {
    std::shared_ptr<Context<TBuffer>> context1 =
        this->helper_->makeContext("ctx1");
    std::shared_ptr<Context<TBuffer>> context2 =
        this->helper_->makeContext("ctx2");
    EXPECT_FALSE(context1->domainDescriptor().empty());
    EXPECT_FALSE(context2->domainDescriptor().empty());
    EXPECT_EQ(context1->domainDescriptor(), context2->domainDescriptor());
    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST_GENERIC(DomainDescriptor);

template <typename TBuffer>
class ClientToServerTest : public ClientServerChannelTestCase<TBuffer> {
 public:
  static constexpr int kDataSize = 256;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kListen);

    // Initialize with sequential values.
    std::vector<uint8_t> data(kDataSize);
    std::iota(data.begin(), data.end(), 0);
    DataWrapper<TBuffer> wrappedData(data);

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) =
        sendWithFuture(channel, wrappedData.buffer());
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
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kConnect);

    DataWrapper<TBuffer> wrappedData(kDataSize);

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    std::future<Error> recvFuture =
        recvWithFuture(channel, descriptor, wrappedData.buffer());
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    auto unwrappedData = wrappedData.unwrap();
    for (auto i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(ClientToServer);

template <typename TBuffer>
class ServerToClientTest : public ClientServerChannelTestCase<TBuffer> {
  static constexpr int kDataSize = 256;

 public:
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kListen);

    DataWrapper<TBuffer> wrappedData(kDataSize);

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kServer);
    std::future<Error> recvFuture =
        recvWithFuture(channel, descriptor, wrappedData.buffer());
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    auto unwrappedData = wrappedData.unwrap();
    for (auto i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i);
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kConnect);

    // Initialize with sequential values.
    std::vector<uint8_t> data(kDataSize);
    std::iota(data.begin(), data.end(), 0);
    DataWrapper<TBuffer> wrappedData(data);

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) =
        sendWithFuture(channel, wrappedData.buffer());
    Error descriptorError;
    TDescriptor descriptor;
    std::tie(descriptorError, descriptor) = descriptorFuture.get();
    EXPECT_FALSE(descriptorError) << descriptorError.what();
    this->peers_->send(PeerGroup::kServer, descriptor);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(ServerToClient);

template <typename TBuffer>
class SendMultipleTensorsTest : public ClientServerChannelTestCase<TBuffer> {
  // FIXME This is very puzzling, as in CircleCI making this field static (and
  // possibly even constexpr) causes a undefined symbol link error.
  const int dataSize_ = 256 * 1024; // 256KB
  static constexpr int kNumTensors = 100;

 public:
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kListen);

    // Initialize with sequential values.
    std::vector<uint8_t> data(dataSize_);
    std::iota(data.begin(), data.end(), 0);
    DataWrapper<TBuffer> wrappedData(data);

    // Error futures
    std::vector<std::future<Error>> sendFutures;

    // Perform send and wait for completion.
    for (int i = 0; i < kNumTensors; i++) {
      std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
      std::future<Error> sendFuture;
      std::tie(descriptorFuture, sendFuture) =
          sendWithFuture(channel, wrappedData.buffer());
      Error descriptorError;
      TDescriptor descriptor;
      std::tie(descriptorError, descriptor) = descriptorFuture.get();
      EXPECT_FALSE(descriptorError) << descriptorError.what();
      this->peers_->send(PeerGroup::kClient, descriptor);
      sendFutures.push_back(std::move(sendFuture));
    }
    for (auto& sendFuture : sendFutures) {
      Error sendError = sendFuture.get();
      EXPECT_FALSE(sendError) << sendError.what();
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kConnect);

    std::vector<DataWrapper<TBuffer>> wrappedDataVec;
    for (int i = 0; i < kNumTensors; i++) {
      wrappedDataVec.emplace_back(dataSize_);
    }

    // Error futures
    std::vector<std::future<Error>> recvFutures;

    // Perform recv and wait for completion.
    for (auto& wrappedData : wrappedDataVec) {
      auto descriptor = this->peers_->recv(PeerGroup::kClient);
      std::future<Error> recvFuture =
          recvWithFuture(channel, descriptor, wrappedData.buffer());
      recvFutures.push_back(std::move(recvFuture));
    }
    for (auto& recvFuture : recvFutures) {
      Error recvError = recvFuture.get();
      EXPECT_FALSE(recvError) << recvError.what();
    }

    // Validate contents of vector.
    for (auto& wrappedData : wrappedDataVec) {
      auto unwrappedData = wrappedData.unwrap();
      for (int i = 0; i < dataSize_; i++) {
        EXPECT_EQ(unwrappedData[i], i % 256);
      }
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(SendMultipleTensors);

template <typename TBuffer>
class SendTensorsBothWaysTest : public ClientServerChannelTestCase<TBuffer> {
  static constexpr int kDataSize = 256;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kListen);

    // Initialize sendBuffer with sequential values.
    std::vector<uint8_t> sendData(kDataSize);
    std::iota(sendData.begin(), sendData.end(), 0);
    DataWrapper<TBuffer> wrappedSendData(sendData);

    // Recv buffer.
    DataWrapper<TBuffer> wrappedRecvData(kDataSize);

    std::future<Error> sendFuture;
    std::future<Error> recvFuture;

    // Perform send.
    {
      std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
      std::tie(descriptorFuture, sendFuture) =
          sendWithFuture(channel, wrappedSendData.buffer());
      Error descriptorError;
      TDescriptor descriptor;
      std::tie(descriptorError, descriptor) = descriptorFuture.get();
      EXPECT_FALSE(descriptorError) << descriptorError.what();
      this->peers_->send(PeerGroup::kClient, descriptor);
    }

    // Perform recv.
    {
      auto descriptor = this->peers_->recv(PeerGroup::kServer);
      recvFuture =
          recvWithFuture(channel, descriptor, wrappedRecvData.buffer());
    }

    // Wait for completion of both.
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Verify recvd buffers.
    auto unwrappedData = wrappedRecvData.unwrap();
    for (int i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i % 256);
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel({std::move(conn)}, Endpoint::kConnect);

    // Initialize sendBuffer with sequential values.
    std::vector<uint8_t> sendData(kDataSize);
    std::iota(sendData.begin(), sendData.end(), 0);
    DataWrapper<TBuffer> wrappedSendData(sendData);

    // Recv buffer.
    DataWrapper<TBuffer> wrappedRecvData(kDataSize);

    std::future<Error> sendFuture;
    std::future<Error> recvFuture;

    // Perform send.
    {
      std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
      std::tie(descriptorFuture, sendFuture) =
          sendWithFuture(channel, wrappedSendData.buffer());
      Error descriptorError;
      TDescriptor descriptor;
      std::tie(descriptorError, descriptor) = descriptorFuture.get();
      EXPECT_FALSE(descriptorError) << descriptorError.what();
      this->peers_->send(PeerGroup::kServer, descriptor);
    }

    // Perform recv.
    {
      auto descriptor = this->peers_->recv(PeerGroup::kClient);
      recvFuture =
          recvWithFuture(channel, descriptor, wrappedRecvData.buffer());
    }

    // Wait for completion of both.
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Verify recvd buffers.
    auto unwrappedData = wrappedRecvData.unwrap();
    for (int i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i % 256);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(SendTensorsBothWays);

template <typename TBuffer>
class ContextIsNotJoinedTest : public ClientServerChannelTestCase<TBuffer> {
  // Because it's static we must define it out-of-line (until C++-17, where we
  // can mark this inline).
  static const std::string kReady;

 public:
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> context =
        this->helper_->makeContext("server");
    this->peers_->send(PeerGroup::kClient, kReady);
    context->createChannel({std::move(conn)}, Endpoint::kListen);
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TBuffer>> context =
        this->helper_->makeContext("client");
    EXPECT_EQ(kReady, this->peers_->recv(PeerGroup::kClient));
    context->createChannel({std::move(conn)}, Endpoint::kConnect);
  }
};

template <typename TBuffer>
const std::string ContextIsNotJoinedTest<TBuffer>::kReady = "ready";

CHANNEL_TEST_GENERIC(ContextIsNotJoined);
