/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cuda_runtime.h>
#include <gmock/gmock.h>

#include <tensorpipe/test/channel/channel_test_cuda.h>
#include <tensorpipe/test/channel/cuda_helpers.h>
#include <tensorpipe/test/test_environment.h>

using namespace tensorpipe;
using namespace tensorpipe::channel;

class SendAcrossDevicesTest : public ClientServerChannelTestCase {
  static constexpr size_t kSize = 1024;

 public:
  void run(ChannelTestHelper* helper) override {
    if (TestEnvironment::numCudaDevices() < 2) {
      GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
    }

    ClientServerChannelTestCase::run(helper);
  }

 private:
  void server(std::shared_ptr<Channel> channel) override {
    cudaStream_t sendStream;
    void* ptr;
    {
      // Send happens from device #0.
      CudaDeviceGuard guard(0);
      TP_CUDA_CHECK(
          cudaStreamCreateWithFlags(&sendStream, cudaStreamNonBlocking));
      TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

      // Set buffer to target value.
      TP_CUDA_CHECK(cudaMemsetAsync(ptr, 0x42, kSize, sendStream));
    }

    // Perform send and wait for completion.
    auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto sendFuture = sendPromise->get_future();

    channel->send(
        CudaBuffer{
            .ptr = ptr,
            .length = 0,
            .stream = sendStream,
        },
        kSize,
        [sendPromise{std::move(sendPromise)}](const tensorpipe::Error& error) {
          sendPromise->set_value(error);
        });

    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    {
      CudaDeviceGuard guard(0);
      TP_CUDA_CHECK(cudaFree(ptr));
      TP_CUDA_CHECK(cudaStreamDestroy(sendStream));
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void afterServer() override {
    if (this->peers_->endpointsInSameProcess()) {
      EXPECT_TRUE(initializedCudaContexts({0, 1}));
    } else {
      EXPECT_TRUE(initializedCudaContexts({0}));
    }
  }

  void client(std::shared_ptr<Channel> channel) override {
    cudaStream_t recvStream;
    void* ptr;
    {
      // Recv happens on device #1.
      CudaDeviceGuard guard(1);
      TP_CUDA_CHECK(
          cudaStreamCreateWithFlags(&recvStream, cudaStreamNonBlocking));
      TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));
    }

    // Perform recv and wait for completion.
    auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto recvFuture = recvPromise->get_future();

    channel->recv(
        CudaBuffer{
            .ptr = ptr,
            .length = 0,
            .stream = recvStream,
        },
        kSize,
        [recvPromise{std::move(recvPromise)}](const tensorpipe::Error& error) {
          recvPromise->set_value(error);
        });

    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    {
      CudaDeviceGuard guard(1);
      std::array<uint8_t, kSize> data;
      TP_CUDA_CHECK(cudaStreamSynchronize(recvStream));
      TP_CUDA_CHECK(cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
      EXPECT_THAT(data, ::testing::Each(0x42));
      TP_CUDA_CHECK(cudaFree(ptr));
      TP_CUDA_CHECK(cudaStreamDestroy(recvStream));
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }

  void afterClient() override {
    if (this->peers_->endpointsInSameProcess()) {
      EXPECT_TRUE(initializedCudaContexts({0, 1}));
    } else {
      EXPECT_TRUE(initializedCudaContexts({1}));
    }
  }
};

CHANNEL_TEST(CudaMultiGPUChannelTestSuite, SendAcrossDevices);

class SendReverseAcrossDevicesTest : public ClientServerChannelTestCase {
  static constexpr size_t kSize = 1024;

 public:
  void run(ChannelTestHelper* helper) override {
    if (TestEnvironment::numCudaDevices() < 2) {
      GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
    }

    ClientServerChannelTestCase::run(helper);
  }

 private:
  void server(std::shared_ptr<Channel> channel) override {
    cudaStream_t sendStream;
    void* ptr;
    {
      // Send happens from device #1.
      CudaDeviceGuard guard(1);
      TP_CUDA_CHECK(
          cudaStreamCreateWithFlags(&sendStream, cudaStreamNonBlocking));
      TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

      // Set buffer to target value.
      TP_CUDA_CHECK(cudaMemsetAsync(ptr, 0x42, kSize, sendStream));
    }

    // Perform send and wait for completion.
    auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto sendFuture = sendPromise->get_future();

    channel->send(
        CudaBuffer{
            .ptr = ptr,
            .length = 0,
            .stream = sendStream,
        },
        kSize,
        [sendPromise{std::move(sendPromise)}](const tensorpipe::Error& error) {
          sendPromise->set_value(error);
        });

    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    {
      CudaDeviceGuard guard(1);
      TP_CUDA_CHECK(cudaFree(ptr));
      TP_CUDA_CHECK(cudaStreamDestroy(sendStream));
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void afterServer() override {
    if (this->peers_->endpointsInSameProcess()) {
      EXPECT_TRUE(initializedCudaContexts({0, 1}));
    } else {
      EXPECT_TRUE(initializedCudaContexts({1}));
    }
  }

  void client(std::shared_ptr<Channel> channel) override {
    cudaStream_t recvStream;
    void* ptr;
    {
      // Recv happens on device #0.
      CudaDeviceGuard guard(0);
      TP_CUDA_CHECK(
          cudaStreamCreateWithFlags(&recvStream, cudaStreamNonBlocking));
      TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));
    }

    // Perform recv and wait for completion.
    auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto recvFuture = recvPromise->get_future();

    channel->recv(
        CudaBuffer{
            .ptr = ptr,
            .length = 0,
            .stream = recvStream,
        },
        kSize,
        [recvPromise{std::move(recvPromise)}](const tensorpipe::Error& error) {
          recvPromise->set_value(error);
        });

    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    {
      CudaDeviceGuard guard(0);
      std::array<uint8_t, kSize> data;
      TP_CUDA_CHECK(cudaStreamSynchronize(recvStream));
      TP_CUDA_CHECK(cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
      EXPECT_THAT(data, ::testing::Each(0x42));
      TP_CUDA_CHECK(cudaFree(ptr));
      TP_CUDA_CHECK(cudaStreamDestroy(recvStream));
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }

  void afterClient() override {
    if (this->peers_->endpointsInSameProcess()) {
      EXPECT_TRUE(initializedCudaContexts({0, 1}));
    } else {
      EXPECT_TRUE(initializedCudaContexts({0}));
    }
  }
};

CHANNEL_TEST(CudaMultiGPUChannelTestSuite, SendReverseAcrossDevices);

class SendAcrossNonDefaultDevicesTest : public ClientServerChannelTestCase {
  static constexpr size_t kSize = 1024;

 public:
  void run(ChannelTestHelper* helper) override {
    if (TestEnvironment::numCudaDevices() < 2) {
      GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
    }

    ClientServerChannelTestCase::run(helper);
  }

 private:
  void server(std::shared_ptr<Channel> channel) override {
    cudaStream_t sendStream;
    void* ptr;
    {
      // Send happens from device #1.
      CudaDeviceGuard guard(1);
      TP_CUDA_CHECK(
          cudaStreamCreateWithFlags(&sendStream, cudaStreamNonBlocking));
      TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

      // Set buffer to target value.
      TP_CUDA_CHECK(cudaMemsetAsync(ptr, 0x42, kSize, sendStream));
    }

    // Perform send and wait for completion.
    auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto sendFuture = sendPromise->get_future();

    channel->send(
        CudaBuffer{
            .ptr = ptr,
            .length = 0,
            .stream = sendStream,
        },
        kSize,
        [sendPromise{std::move(sendPromise)}](const tensorpipe::Error& error) {
          sendPromise->set_value(error);
        });

    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    {
      CudaDeviceGuard guard(1);
      TP_CUDA_CHECK(cudaFree(ptr));
      TP_CUDA_CHECK(cudaStreamDestroy(sendStream));
    }

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void afterServer() override {
    EXPECT_TRUE(initializedCudaContexts({1}));
  }

  void client(std::shared_ptr<Channel> channel) override {
    cudaStream_t recvStream;
    void* ptr;
    {
      // Recv happens on device #1.
      CudaDeviceGuard guard(1);
      TP_CUDA_CHECK(
          cudaStreamCreateWithFlags(&recvStream, cudaStreamNonBlocking));
      TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));
    }

    // Perform recv and wait for completion.
    auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto recvFuture = recvPromise->get_future();

    channel->recv(
        CudaBuffer{
            .ptr = ptr,
            .length = 0,
            .stream = recvStream,
        },
        kSize,
        [recvPromise{std::move(recvPromise)}](const tensorpipe::Error& error) {
          recvPromise->set_value(error);
        });

    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    {
      CudaDeviceGuard guard(1);
      std::array<uint8_t, kSize> data;
      TP_CUDA_CHECK(cudaStreamSynchronize(recvStream));
      TP_CUDA_CHECK(cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
      EXPECT_THAT(data, ::testing::Each(0x42));
      TP_CUDA_CHECK(cudaFree(ptr));
      TP_CUDA_CHECK(cudaStreamDestroy(recvStream));
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }

  void afterClient() override {
    EXPECT_TRUE(initializedCudaContexts({1}));
  }
};

CHANNEL_TEST(CudaMultiGPUChannelTestSuite, SendAcrossNonDefaultDevices);
