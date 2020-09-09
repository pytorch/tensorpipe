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

template <typename TTensor>
class DomainDescriptorTest : public ChannelTest<TTensor> {
 public:
  void run(ChannelTestHelper<TTensor>* helper) override {
    std::shared_ptr<Context<TTensor>> context1 = helper->makeContext("ctx1");
    std::shared_ptr<Context<TTensor>> context2 = helper->makeContext("ctx2");
    EXPECT_FALSE(context1->domainDescriptor().empty());
    EXPECT_FALSE(context2->domainDescriptor().empty());
    EXPECT_EQ(context1->domainDescriptor(), context2->domainDescriptor());
  }
};

CHANNEL_TEST_GENERIC(DomainDescriptor);

template <typename TTensor>
class ClientToServerTest : public ChannelTest<TTensor> {
 public:
  static constexpr int dataSize = 256;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Initialize with sequential values.
    std::vector<uint8_t> data(dataSize);
    std::iota(data.begin(), data.end(), 0);
    DataWrapper<TTensor> wrappedData(data);

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) =
        sendWithFuture(channel, wrappedData.tensor());
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
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    DataWrapper<TTensor> wrappedData(dataSize);

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    std::future<Error> recvFuture =
        recvWithFuture(channel, descriptor, wrappedData.tensor());
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    auto unwrappedData = wrappedData.unwrap();
    for (auto i = 0; i < dataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(ClientToServer);

// TEST_P(ChannelTest, ServerToClient) {
template <typename TTensor>
class ServerToClientTest : public ChannelTest<TTensor> {
  static constexpr int dataSize = 256;

 public:
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    DataWrapper<TTensor> wrappedData(dataSize);

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kServer);
    std::future<Error> recvFuture =
        recvWithFuture(channel, descriptor, wrappedData.tensor());
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    auto unwrappedData = wrappedData.unwrap();
    for (auto i = 0; i < dataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i);
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Initialize with sequential values.
    std::vector<uint8_t> data(dataSize);
    std::iota(data.begin(), data.end(), 0);
    DataWrapper<TTensor> wrappedData(data);

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) =
        sendWithFuture(channel, wrappedData.tensor());
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

template <typename TTensor>
class SendMultipleTensorsTest : public ChannelTest<TTensor> {
  const int dataSize = 256 * 1024; // 256KB
  static constexpr int numTensors = 100;

 public:
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Initialize with sequential values.
    std::vector<uint8_t> data(dataSize);
    std::iota(data.begin(), data.end(), 0);
    DataWrapper<TTensor> wrappedData(data);

    // Error futures
    std::vector<std::future<Error>> sendFutures;

    // Perform send and wait for completion.
    for (int i = 0; i < numTensors; i++) {
      std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
      std::future<Error> sendFuture;
      std::tie(descriptorFuture, sendFuture) =
          sendWithFuture(channel, wrappedData.tensor());
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
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    std::vector<std::unique_ptr<DataWrapper<TTensor>>> wrappedDataVec;
    for (int i = 0; i < numTensors; i++) {
      wrappedDataVec.push_back(
          std::make_unique<DataWrapper<TTensor>>(dataSize));
    }

    // Error futures
    std::vector<std::future<Error>> recvFutures;

    // Perform recv and wait for completion.
    for (auto& wrappedData : wrappedDataVec) {
      auto descriptor = this->peers_->recv(PeerGroup::kClient);
      std::future<Error> recvFuture =
          recvWithFuture(channel, descriptor, wrappedData->tensor());
      recvFutures.push_back(std::move(recvFuture));
    }
    for (auto& recvFuture : recvFutures) {
      Error recvError = recvFuture.get();
      EXPECT_FALSE(recvError) << recvError.what();
    }

    // Validate contents of vector.
    for (auto& wrappedData : wrappedDataVec) {
      auto unwrappedData = wrappedData->unwrap();
      for (int i = 0; i < dataSize; i++) {
        EXPECT_EQ(unwrappedData[i], i % 256);
      }
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(SendMultipleTensors);

template <typename TTensor>
class SendTensorsBothWaysTest : public ChannelTest<TTensor> {
  static constexpr int dataSize = 256;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Initialize sendBuffer with sequential values.
    std::vector<uint8_t> sendData(dataSize);
    std::iota(sendData.begin(), sendData.end(), 0);
    DataWrapper<TTensor> wrappedSendData(sendData);

    // Recv buffer.
    DataWrapper<TTensor> wrappedRecvData(dataSize);

    std::future<Error> sendFuture;
    std::future<Error> recvFuture;

    // Perform send.
    {
      std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
      std::tie(descriptorFuture, sendFuture) =
          sendWithFuture(channel, wrappedSendData.tensor());
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
          recvWithFuture(channel, descriptor, wrappedRecvData.tensor());
    }

    // Wait for completion of both.
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Verify recvd buffers.
    auto unwrappedData = wrappedRecvData.unwrap();
    for (int i = 0; i < dataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i % 256);
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Initialize sendBuffer with sequential values.
    std::vector<uint8_t> sendData(dataSize);
    std::iota(sendData.begin(), sendData.end(), 0);
    DataWrapper<TTensor> wrappedSendData(sendData);

    // Recv buffer.
    DataWrapper<TTensor> wrappedRecvData(dataSize);

    std::future<Error> sendFuture;
    std::future<Error> recvFuture;

    // Perform send.
    {
      std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
      std::tie(descriptorFuture, sendFuture) =
          sendWithFuture(channel, wrappedSendData.tensor());
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
          recvWithFuture(channel, descriptor, wrappedRecvData.tensor());
    }

    // Wait for completion of both.
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Verify recvd buffers.
    auto unwrappedData = wrappedRecvData.unwrap();
    for (int i = 0; i < dataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i % 256);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(SendTensorsBothWays);

template <typename TTensor>
class NullPointerTest : public ChannelTest<TTensor> {
  // Call send and recv with a null pointer and a length of 0.

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) = sendWithFuture(
        channel,
        TTensor{
            .ptr = nullptr,
            .length = 0,
        });
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
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    std::future<Error> recvFuture = recvWithFuture(
        channel,
        descriptor,
        TTensor{
            .ptr = nullptr,
            .length = 0,
        });
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

// Call send and recv with a length of 0 but a non-null pointer.
template <typename TTensor>
class EmptyTensorTest : public ChannelTest<TTensor> {
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Allocate a non-empty vector so that its .data() pointer is non-null.
    std::vector<uint8_t> data(1);
    DataWrapper<TTensor> wrappedData(data);
    TTensor tensor = wrappedData.tensor();
    tensor.length = 0;

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) = sendWithFuture(channel, tensor);
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
    std::shared_ptr<Context<TTensor>> ctx =
        this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Allocate a non-empty vector so that its .data() pointer is non-null.
    DataWrapper<TTensor> wrappedData(1);
    TTensor tensor = wrappedData.tensor();
    tensor.length = 0;

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    std::future<Error> recvFuture = recvWithFuture(channel, descriptor, tensor);
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST_GENERIC(EmptyTensor);

template <typename TTensor>
class ContextIsNotJoinedTest : public ChannelTest<TTensor> {
  const std::string kReady = "ready";

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> context =
        this->helper_->makeContext("server");
    this->peers_->send(PeerGroup::kClient, kReady);
    context->createChannel(std::move(conn), Endpoint::kListen);
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<Context<TTensor>> context =
        this->helper_->makeContext("client");
    EXPECT_EQ(kReady, this->peers_->recv(PeerGroup::kClient));
    context->createChannel(std::move(conn), Endpoint::kConnect);
  }
};

CHANNEL_TEST_GENERIC(ContextIsNotJoined);

// This test wants to make sure that the "heavy lifting" of copying data isn't
// performed inline inside the recv method as that would make the user-facing
// read method of the pipe blocking.
// However, since we can't really check that behavior, we'll check a highly
// correlated one: that the recv callback isn't called inline from within the
// recv method. We do so by having that behavior cause a deadlock.
class CallbacksAreDeferredTest : public ChannelTest<tensorpipe::CpuTensor> {
  static constexpr auto dataSize = 256;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CpuContext> ctx = this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Initialize with sequential values.
    std::vector<uint8_t> data(dataSize);
    std::iota(data.begin(), data.end(), 0);

    // Perform send and wait for completion.
    std::promise<std::tuple<Error, TDescriptor>> descriptorPromise;
    std::promise<Error> sendPromise;
    std::mutex mutex;
    std::unique_lock<std::mutex> callerLock(mutex);
    channel->send(
        CpuTensor{
            .ptr = data.data(),
            .length = data.size(),
        },
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
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Initialize with zeroes.
    std::vector<uint8_t> data(dataSize);
    std::fill(data.begin(), data.end(), 0);

    // Perform recv and wait for completion.
    std::promise<Error> recvPromise;
    std::mutex mutex;
    std::unique_lock<std::mutex> callerLock(mutex);
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    channel->recv(
        descriptor,
        CpuTensor{
            .ptr = data.data(),
            .length = data.size(),
        },
        [&recvPromise, &mutex](const Error& error) {
          std::unique_lock<std::mutex> calleeLock(mutex);
          recvPromise.set_value(error);
        });
    callerLock.unlock();
    Error recvError = recvPromise.get_future().get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    for (auto i = 0; i < dataSize; i++) {
      EXPECT_EQ(data[i], i);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(Cpu, CallbacksAreDeferred);
