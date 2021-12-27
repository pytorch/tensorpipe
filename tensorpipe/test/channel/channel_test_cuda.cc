/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/channel_test_cuda.h>

#include <cuda_runtime.h>
#include <gmock/gmock.h>

#include <tensorpipe/test/channel/kernel.cuh>

using namespace tensorpipe;
using namespace tensorpipe::channel;

class ReceiverWaitsForStartEventTest : public ClientServerChannelTestCase {
  static constexpr size_t kSize = 1024;

  void server(std::shared_ptr<Channel> channel) override {
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
    auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto sendFuture = sendPromise->get_future();

    channel->send(
        CudaBuffer{
            .ptr = ptr,
            .stream = sendStream,
        },
        kSize,
        [sendPromise{std::move(sendPromise)}](const tensorpipe::Error& error) {
          sendPromise->set_value(error);
        });

    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();
    TP_CUDA_CHECK(cudaFree(ptr));

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    TP_CUDA_CHECK(cudaSetDevice(0));
    cudaStream_t recvStream;
    TP_CUDA_CHECK(
        cudaStreamCreateWithFlags(&recvStream, cudaStreamNonBlocking));
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

    // Perform recv and wait for completion.
    auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto recvFuture = recvPromise->get_future();

    channel->recv(
        CudaBuffer{
            .ptr = ptr,
            .stream = recvStream,
        },
        kSize,
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
  }
};

CHANNEL_TEST(CudaChannelTestSuite, ReceiverWaitsForStartEvent);

class SendOffsetAllocationTest : public ClientServerChannelTestCase {
 public:
  static constexpr int kDataSize = 256;
  static constexpr int kOffset = 128;

  void server(std::shared_ptr<Channel> channel) override {
    // Initialize with sequential values.
    void* ptr;
    TP_CUDA_CHECK(cudaMalloc(&ptr, kOffset + kDataSize));
    // Set buffer to target value.
    TP_CUDA_CHECK(cudaMemset(ptr, 0xff, kOffset));
    TP_CUDA_CHECK(
        cudaMemset(static_cast<uint8_t*>(ptr) + kOffset, 0x42, kDataSize));

    // Perform send and wait for completion.
    std::future<Error> sendFuture = sendWithFuture(
        channel,
        CudaBuffer{.ptr = static_cast<uint8_t*>(ptr) + kOffset},
        kDataSize);
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
    EXPECT_THAT(wrappedData->unwrap(), ::testing::Each(0x42));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(CudaChannelTestSuite, SendOffsetAllocation);
