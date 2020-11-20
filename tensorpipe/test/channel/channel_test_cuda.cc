/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cuda_runtime.h>

#include <tensorpipe/channel/cuda_context.h>
#include <tensorpipe/test/channel/channel_test.h>
#include <tensorpipe/test/test_environment.h>
#include <tensorpipe/test/channel/kernel.cuh>

using namespace tensorpipe;
using namespace tensorpipe::channel;

class ReceiverWaitsForStartEventTest
    : public ClientServerChannelTestCase<CudaBuffer> {
  static constexpr size_t kSize = 1024;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    EXPECT_EQ(cudaSuccess, cudaSetDevice(0));
    cudaStream_t sendStream;
    EXPECT_EQ(cudaSuccess, cudaStreamCreate(&sendStream));
    void* ptr;
    EXPECT_EQ(cudaSuccess, cudaMalloc(&ptr, kSize));

    // Delay sendStream with computations on buffer.
    slowKernel(ptr, kSize, sendStream);

    // Set buffer to target value.
    EXPECT_EQ(cudaSuccess, cudaMemsetAsync(ptr, 0x42, kSize, sendStream));

    // Perform send and wait for completion.
    auto descriptorPromise = std::make_shared<
        std::promise<std::tuple<tensorpipe::Error, std::string>>>();
    auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto descriptorFuture = descriptorPromise->get_future();
    auto sendFuture = sendPromise->get_future();

    channel->send(
        CudaBuffer{
            .ptr = ptr,
            .length = kSize,
            .stream = sendStream,
        },
        [descriptorPromise{std::move(descriptorPromise)}](
            const tensorpipe::Error& error, std::string descriptor) {
          descriptorPromise->set_value(
              std::make_tuple(error, std::move(descriptor)));
        },
        [sendPromise{std::move(sendPromise)}](const tensorpipe::Error& error) {
          sendPromise->set_value(error);
        });

    Error descriptorError;
    TDescriptor descriptor;
    std::tie(descriptorError, descriptor) = descriptorFuture.get();

    EXPECT_FALSE(descriptorError) << descriptorError.what();
    this->peers_->send(PeerGroup::kClient, descriptor);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    EXPECT_EQ(cudaSuccess, cudaFree(ptr));

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    EXPECT_EQ(cudaSuccess, cudaSetDevice(0));
    cudaStream_t recvStream;
    EXPECT_EQ(cudaSuccess, cudaStreamCreate(&recvStream));
    void* ptr;
    EXPECT_EQ(cudaSuccess, cudaMalloc(&ptr, kSize));

    auto descriptor = this->peers_->recv(PeerGroup::kClient);

    // Perform recv and wait for completion.
    auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto recvFuture = recvPromise->get_future();

    channel->recv(
        std::move(descriptor),
        CudaBuffer{
            .ptr = ptr,
            .length = kSize,
            .stream = recvStream,
        },
        [recvPromise{std::move(recvPromise)}](const tensorpipe::Error& error) {
          recvPromise->set_value(error);
        });

    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    std::array<uint8_t, kSize> data;
    EXPECT_EQ(
        cudaSuccess, cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
    // Validate contents of vector.
    for (auto i = 0; i < kSize; i++) {
      EXPECT_EQ(data[i], 0x42);
    }
    EXPECT_EQ(cudaSuccess, cudaFree(ptr));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CudaChannelTestSuite, ReceiverWaitsForStartEvent);

class SendAcrossDevicesTest : public ClientServerChannelTestCase<CudaBuffer> {
  static constexpr size_t kSize = 1024;

 public:
  void run(ChannelTestHelper<CudaBuffer>* helper) override {
    if (TestEnvironment::numCudaDevices() < 2) {
      GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
    }

    ClientServerChannelTestCase<CudaBuffer>::run(helper);
  }

 private:
  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Send happens from device #0.
    EXPECT_EQ(cudaSuccess, cudaSetDevice(0));
    cudaStream_t sendStream;
    EXPECT_EQ(cudaSuccess, cudaStreamCreate(&sendStream));
    void* ptr;
    EXPECT_EQ(cudaSuccess, cudaMalloc(&ptr, kSize));

    // Set buffer to target value.
    EXPECT_EQ(cudaSuccess, cudaMemsetAsync(ptr, 0x42, kSize, sendStream));

    // Perform send and wait for completion.
    auto descriptorPromise = std::make_shared<
        std::promise<std::tuple<tensorpipe::Error, std::string>>>();
    auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto descriptorFuture = descriptorPromise->get_future();
    auto sendFuture = sendPromise->get_future();

    channel->send(
        CudaBuffer{
            .ptr = ptr,
            .length = kSize,
            .stream = sendStream,
        },
        [descriptorPromise{std::move(descriptorPromise)}](
            const tensorpipe::Error& error, std::string descriptor) {
          descriptorPromise->set_value(
              std::make_tuple(error, std::move(descriptor)));
        },
        [sendPromise{std::move(sendPromise)}](const tensorpipe::Error& error) {
          sendPromise->set_value(error);
        });

    Error descriptorError;
    TDescriptor descriptor;
    std::tie(descriptorError, descriptor) = descriptorFuture.get();

    EXPECT_FALSE(descriptorError) << descriptorError.what();
    this->peers_->send(PeerGroup::kClient, descriptor);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    EXPECT_EQ(cudaSuccess, cudaFree(ptr));

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Recv happens on device #1.
    EXPECT_EQ(cudaSuccess, cudaSetDevice(1));
    cudaStream_t recvStream;
    EXPECT_EQ(cudaSuccess, cudaStreamCreate(&recvStream));
    void* ptr;
    EXPECT_EQ(cudaSuccess, cudaMalloc(&ptr, kSize));

    auto descriptor = this->peers_->recv(PeerGroup::kClient);

    // Perform recv and wait for completion.
    auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto recvFuture = recvPromise->get_future();

    channel->recv(
        std::move(descriptor),
        CudaBuffer{
            .ptr = ptr,
            .length = kSize,
            .stream = recvStream,
        },
        [recvPromise{std::move(recvPromise)}](const tensorpipe::Error& error) {
          recvPromise->set_value(error);
        });

    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    std::array<uint8_t, kSize> data;
    EXPECT_EQ(
        cudaSuccess, cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
    // Validate contents of vector.
    for (auto i = 0; i < kSize; i++) {
      EXPECT_EQ(data[i], 0x42);
    }
    EXPECT_EQ(cudaSuccess, cudaFree(ptr));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CudaChannelTestSuite, SendAcrossDevices);
