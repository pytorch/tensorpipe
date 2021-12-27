/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/channel_test.h>

#include <numeric>

using namespace tensorpipe;
using namespace tensorpipe::channel;

// Implement this in a subprocess as in some cases it may initialize CUDA and
// thus would otherwise "pollute" the parent process.
class DeviceDescriptorsTest : public ChannelTestCase {
 public:
  void run(ChannelTestHelper* helper) override {
    auto peerGroup = helper->makePeerGroup();
    peerGroup->spawn(
        [&] {
          std::shared_ptr<Context> context1 = helper->makeContext("ctx1");
          std::shared_ptr<Context> context2 = helper->makeContext("ctx2");
          const auto& descriptors1 = context1->deviceDescriptors();
          const auto& descriptors2 = context2->deviceDescriptors();

          EXPECT_FALSE(descriptors1.empty());
          EXPECT_FALSE(descriptors2.empty());

          EXPECT_EQ(descriptors1.size(), descriptors2.size());
          for (const auto& deviceIter : descriptors1) {
            EXPECT_FALSE(deviceIter.second.empty());
            EXPECT_EQ(descriptors2.count(deviceIter.first), 1);
            EXPECT_EQ(deviceIter.second, descriptors2.at(deviceIter.first));
          }
        },
        [] {});
  }
};

CHANNEL_TEST(ChannelTestSuite, DeviceDescriptors);

class ClientToServerTest : public ClientServerChannelTestCase {
 public:
  static constexpr int kDataSize = 256;

  void server(std::shared_ptr<Channel> channel) override {
    // Initialize with sequential values.
    std::vector<uint8_t> data(kDataSize);
    std::iota(data.begin(), data.end(), 0);
    std::unique_ptr<DataWrapper> wrappedData = helper_->makeDataWrapper(data);

    // Perform send and wait for completion.
    std::future<Error> sendFuture = sendWithFuture(channel, *wrappedData);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    std::unique_ptr<DataWrapper> wrappedData =
        helper_->makeDataWrapper(kDataSize);

    // Perform recv and wait for completion.
    std::future<Error> recvFuture = recvWithFuture(channel, *wrappedData);
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    auto unwrappedData = wrappedData->unwrap();
    for (auto i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(ChannelTestSuite, ClientToServer);

class ServerToClientTest : public ClientServerChannelTestCase {
  static constexpr int kDataSize = 256;

 public:
  void server(std::shared_ptr<Channel> channel) override {
    std::unique_ptr<DataWrapper> wrappedData =
        helper_->makeDataWrapper(kDataSize);

    // Perform recv and wait for completion.
    std::future<Error> recvFuture = recvWithFuture(channel, *wrappedData);
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    auto unwrappedData = wrappedData->unwrap();
    for (auto i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i);
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    // Initialize with sequential values.
    std::vector<uint8_t> data(kDataSize);
    std::iota(data.begin(), data.end(), 0);
    std::unique_ptr<DataWrapper> wrappedData = helper_->makeDataWrapper(data);

    // Perform send and wait for completion.
    std::future<Error> sendFuture = sendWithFuture(channel, *wrappedData);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(ChannelTestSuite, ServerToClient);

class SendMultipleTensorsTest : public ClientServerChannelTestCase {
  // FIXME This is very puzzling, as in CircleCI making this field static (and
  // possibly even constexpr) causes a undefined symbol link error.
  const int dataSize_ = 256 * 1024; // 256KB
  static constexpr int kNumTensors = 100;

 public:
  void server(std::shared_ptr<Channel> channel) override {
    // Initialize with sequential values.
    std::vector<uint8_t> data(dataSize_);
    std::iota(data.begin(), data.end(), 0);
    std::unique_ptr<DataWrapper> wrappedData = helper_->makeDataWrapper(data);

    // Error futures
    std::vector<std::future<Error>> sendFutures;

    // Perform send and wait for completion.
    for (int i = 0; i < kNumTensors; i++) {
      std::future<Error> sendFuture = sendWithFuture(channel, *wrappedData);
      sendFutures.push_back(std::move(sendFuture));
    }
    for (auto& sendFuture : sendFutures) {
      Error sendError = sendFuture.get();
      EXPECT_FALSE(sendError) << sendError.what();
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    std::vector<std::unique_ptr<DataWrapper>> wrappedDataVec;
    for (int i = 0; i < kNumTensors; i++) {
      wrappedDataVec.push_back(helper_->makeDataWrapper(dataSize_));
    }

    // Error futures
    std::vector<std::future<Error>> recvFutures;

    // Perform recv and wait for completion.
    for (auto& wrappedData : wrappedDataVec) {
      std::future<Error> recvFuture = recvWithFuture(channel, *wrappedData);
      recvFutures.push_back(std::move(recvFuture));
    }
    for (auto& recvFuture : recvFutures) {
      Error recvError = recvFuture.get();
      EXPECT_FALSE(recvError) << recvError.what();
    }

    // Validate contents of vector.
    for (auto& wrappedData : wrappedDataVec) {
      auto unwrappedData = wrappedData->unwrap();
      for (int i = 0; i < dataSize_; i++) {
        EXPECT_EQ(unwrappedData[i], i % 256);
      }
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(ChannelTestSuite, SendMultipleTensors);

class SendTensorsBothWaysTest : public ClientServerChannelTestCase {
  static constexpr int kDataSize = 256;

  void server(std::shared_ptr<Channel> channel) override {
    // Initialize sendBuffer with sequential values.
    std::vector<uint8_t> sendData(kDataSize);
    std::iota(sendData.begin(), sendData.end(), 0);
    std::unique_ptr<DataWrapper> wrappedSendData =
        helper_->makeDataWrapper(sendData);

    // Recv buffer.
    std::unique_ptr<DataWrapper> wrappedRecvData =
        helper_->makeDataWrapper(kDataSize);

    // Perform send.
    std::future<Error> sendFuture = sendWithFuture(channel, *wrappedSendData);
    // Perform recv.
    std::future<Error> recvFuture = recvWithFuture(channel, *wrappedRecvData);

    // Wait for completion of both.
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Verify recvd buffers.
    auto unwrappedData = wrappedRecvData->unwrap();
    for (int i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i % 256);
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    // Initialize sendBuffer with sequential values.
    std::vector<uint8_t> sendData(kDataSize);
    std::iota(sendData.begin(), sendData.end(), 0);
    std::unique_ptr<DataWrapper> wrappedSendData =
        helper_->makeDataWrapper(sendData);

    // Recv buffer.
    std::unique_ptr<DataWrapper> wrappedRecvData =
        helper_->makeDataWrapper(kDataSize);

    // Perform send.
    std::future<Error> sendFuture = sendWithFuture(channel, *wrappedSendData);
    // Perform recv.
    std::future<Error> recvFuture = recvWithFuture(channel, *wrappedRecvData);

    // Wait for completion of both.
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Verify recvd buffers.
    auto unwrappedData = wrappedRecvData->unwrap();
    for (int i = 0; i < kDataSize; i++) {
      EXPECT_EQ(unwrappedData[i], i % 256);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(ChannelTestSuite, SendTensorsBothWays);

// Call send and recv with a length of 0 but a non-null pointer.
class EmptyTensorTest : public ClientServerChannelTestCase {
  void server(std::shared_ptr<Channel> channel) override {
    // Allocate a non-empty vector so that its .data() pointer is non-null.
    std::vector<uint8_t> data(1);
    std::unique_ptr<DataWrapper> wrappedData = helper_->makeDataWrapper(data);
    Buffer buffer = wrappedData->buffer();

    // Perform send and wait for completion.
    std::future<Error> sendFuture = sendWithFuture(channel, buffer, 0);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    // Allocate a non-empty vector so that its .data() pointer is non-null.
    std::unique_ptr<DataWrapper> wrappedData = helper_->makeDataWrapper(1);
    Buffer buffer = wrappedData->buffer();

    // Perform recv and wait for completion.
    std::future<Error> recvFuture = recvWithFuture(channel, buffer, 0);
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(ChannelTestSuite, EmptyTensor);

// Call send and recv with a length of 0, between sends and recvs with
// positive length.
class EmptyAndNonEmptyTensorsTest : public ClientServerChannelTestCase {
  void server(std::shared_ptr<Channel> channel) override {
    std::vector<uint8_t> data(1);
    std::unique_ptr<DataWrapper> wrappedData = helper_->makeDataWrapper(data);
    Buffer buffer = wrappedData->buffer();

    std::vector<std::future<Error>> sendFutures;
    sendFutures.push_back(sendWithFuture(channel, buffer, 1));
    sendFutures.push_back(sendWithFuture(channel, buffer, 0));
    sendFutures.push_back(sendWithFuture(channel, buffer, 1));

    for (auto& f : sendFutures) {
      Error sendError = f.get();
      EXPECT_FALSE(sendError) << sendError.what();
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    std::unique_ptr<DataWrapper> wrappedData = helper_->makeDataWrapper(1);
    Buffer buffer = wrappedData->buffer();

    std::vector<std::future<Error>> sendFutures;
    sendFutures.push_back(recvWithFuture(channel, buffer, 1));
    sendFutures.push_back(recvWithFuture(channel, buffer, 0));
    sendFutures.push_back(recvWithFuture(channel, buffer, 1));

    for (auto& f : sendFutures) {
      Error sendError = f.get();
      EXPECT_FALSE(sendError) << sendError.what();
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(ChannelTestSuite, EmptyAndNonEmptyTensors);
