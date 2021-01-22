/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cuda_runtime.h>
#include <gmock/gmock.h>

#include <tensorpipe/channel/cuda_context.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/test/channel/channel_test.h>
#include <tensorpipe/test/test_environment.h>

using namespace tensorpipe;
using namespace tensorpipe::channel;

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
    TP_CUDA_CHECK(cudaSetDevice(0));
    cudaStream_t sendStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&sendStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

    // Set buffer to target value.
    TP_CUDA_CHECK(cudaMemsetAsync(ptr, 0x42, kSize, sendStream));

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
    TP_CUDA_CHECK(cudaFree(ptr));

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Recv happens on device #1.
    TP_CUDA_CHECK(cudaSetDevice(1));
    cudaStream_t recvStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&recvStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

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
    TP_CUDA_CHECK(cudaStreamSynchronize(recvStream));
    TP_CUDA_CHECK(cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
    EXPECT_THAT(data, ::testing::Each(0x42));
    TP_CUDA_CHECK(cudaFree(ptr));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CudaMultiGPUChannelTestSuite, SendAcrossDevices);

class SendReverseAcrossDevicesTest
    : public ClientServerChannelTestCase<CudaBuffer> {
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

    // Send happens from device #1.
    TP_CUDA_CHECK(cudaSetDevice(1));
    cudaStream_t sendStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&sendStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

    // Set buffer to target value.
    TP_CUDA_CHECK(cudaMemsetAsync(ptr, 0x42, kSize, sendStream));

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
    TP_CUDA_CHECK(cudaFree(ptr));

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Recv happens on device #0.
    TP_CUDA_CHECK(cudaSetDevice(0));
    cudaStream_t recvStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&recvStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

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
    TP_CUDA_CHECK(cudaStreamSynchronize(recvStream));
    TP_CUDA_CHECK(cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
    EXPECT_THAT(data, ::testing::Each(0x42));
    TP_CUDA_CHECK(cudaFree(ptr));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CudaMultiGPUChannelTestSuite, SendReverseAcrossDevices);

class SendAcrossNonDefaultDevicesTest
    : public ClientServerChannelTestCase<CudaBuffer> {
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

    // Send happens from device #1.
    TP_CUDA_CHECK(cudaSetDevice(1));
    cudaStream_t sendStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&sendStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

    // Set buffer to target value.
    TP_CUDA_CHECK(cudaMemsetAsync(ptr, 0x42, kSize, sendStream));

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
    TP_CUDA_CHECK(cudaFree(ptr));

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);

    ctx->join();
  }

  void client(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    // Recv happens on device #1.
    TP_CUDA_CHECK(cudaSetDevice(1));
    cudaStream_t recvStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&recvStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

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
    TP_CUDA_CHECK(cudaStreamSynchronize(recvStream));
    TP_CUDA_CHECK(cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
    EXPECT_THAT(data, ::testing::Each(0x42));
    TP_CUDA_CHECK(cudaFree(ptr));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CudaMultiGPUChannelTestSuite, SendAcrossNonDefaultDevices);
