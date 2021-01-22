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
#include <tensorpipe/test/channel/kernel.cuh>

using namespace tensorpipe;
using namespace tensorpipe::channel;

class ReceiverWaitsForStartEventTest
    : public ClientServerChannelTestCase<CudaBuffer> {
  static constexpr size_t kSize = 1024;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    TP_CUDA_CHECK(cudaSetDevice(0));
    cudaStream_t sendStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&sendStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

    // Delay sendStream with computations on buffer.
    slowKernel(ptr, kSize, sendStream);

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

CHANNEL_TEST(CudaChannelTestSuite, ReceiverWaitsForStartEvent);

class SendOffsetAllocationTest
    : public ClientServerChannelTestCase<CudaBuffer> {
 public:
  static constexpr int kDataSize = 256;
  static constexpr int kOffset = 128;

  void server(std::shared_ptr<transport::Connection> conn) override {
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("server");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kListen);

    // Initialize with sequential values.
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kOffset + kDataSize));
    // Set buffer to target value.
    TP_CUDA_CHECK(cudaMemset(ptr, 0xff, kOffset));
    TP_CUDA_CHECK(
        cudaMemset(static_cast<uint8_t*>(ptr) + kOffset, 0x42, kDataSize));

    // Perform send and wait for completion.
    std::future<std::tuple<Error, TDescriptor>> descriptorFuture;
    std::future<Error> sendFuture;
    std::tie(descriptorFuture, sendFuture) = sendWithFuture(
        channel, CudaBuffer{static_cast<uint8_t*>(ptr) + kOffset, kDataSize});
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
    std::shared_ptr<CudaContext> ctx = this->helper_->makeContext("client");
    auto channel = ctx->createChannel(std::move(conn), Endpoint::kConnect);

    DataWrapper<CudaBuffer> wrappedData(kDataSize);

    // Perform recv and wait for completion.
    auto descriptor = this->peers_->recv(PeerGroup::kClient);
    std::future<Error> recvFuture =
        recvWithFuture(channel, descriptor, wrappedData.buffer());
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    EXPECT_THAT(wrappedData.unwrap(), ::testing::Each(0x42));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);

    ctx->join();
  }
};

CHANNEL_TEST(CudaChannelTestSuite, SendOffsetAllocation);
