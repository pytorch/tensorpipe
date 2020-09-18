/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <cuda_runtime.h>

#include <tensorpipe/channel/cuda_ipc/channel.h>
#include <tensorpipe/channel/cuda_ipc/context.h>
#include <tensorpipe/test/channel/channel_test.h>
#include <tensorpipe/test/channel/cuda_ipc/kernel.cuh>

namespace {

class CudaWrapper : public DataWrapper {
 public:
  explicit CudaWrapper(size_t len) : size_(len) {
    EXPECT_EQ(cudaSuccess, cudaSetDevice(0));
    if (size_ > 0) {
      EXPECT_EQ(cudaSuccess, cudaMalloc(&cudaData_, size_));
    }
  }

  void* data() override {
    return cudaData_;
  }

  size_t size() override {
    return size_;
  }

  void wrap(const void* ptr) override {
    if (size_ > 0) {
      EXPECT_EQ(
          cudaSuccess, cudaMemcpy(cudaData_, ptr, size_, cudaMemcpyDefault));
    }
  }

  void unwrap(void* ptr) override {
    if (size_ > 0) {
      EXPECT_EQ(
          cudaSuccess, cudaMemcpy(ptr, cudaData_, size_, cudaMemcpyDefault));
    }
  }

  ~CudaWrapper() override {
    if (size_ > 0) {
      EXPECT_EQ(cudaSuccess, cudaFree(cudaData_));
    }
  }

 private:
  void* cudaData_;
  size_t size_;
};

class CudaChannelTestHelper : public ChannelTestHelper {
 public:
  std::string channelName() override {
    return "cuda_ipc";
  }

  std::shared_ptr<tensorpipe::channel::Context> makeContext(
      std::string id) override {
    auto context = std::make_shared<tensorpipe::channel::cuda_ipc::Context>();
    context->setId(std::move(id));
    return context;
  }

  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ProcessPeerGroup>();
  }

  std::shared_ptr<DataWrapper> makeBuffer(size_t len) override {
    return std::make_shared<CudaWrapper>(len);
  }
};

CudaChannelTestHelper helper;

class CudaIpcChannelTest : public ChannelTest {};

} // namespace

using namespace tensorpipe;
using namespace tensorpipe::channel;

#define TP_CUDA_CHECK(a)                                                      \
  TP_THROW_ASSERT_IF(cudaSuccess != (a))                                      \
      << __TP_EXPAND_OPD(a) << " " << cudaGetErrorName(cudaPeekAtLastError()) \
      << " (" << cudaGetErrorString(cudaPeekAtLastError()) << ")"

TEST_P(CudaIpcChannelTest, ReceiverWaitsForStartEvent) {
  constexpr int kSize = 1024;

  testConnection(
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("server");
        auto channel = std::static_pointer_cast<cuda_ipc::Channel>(
            ctx->createChannel(std::move(conn), Endpoint::kListen));

        TP_CUDA_CHECK(cudaSetDevice(0));
        cudaStream_t sendStream;
        TP_CUDA_CHECK(cudaStreamCreate(&sendStream));
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
            ptr,
            kSize,
            [descriptorPromise{std::move(descriptorPromise)}](
                const tensorpipe::Error& error, std::string descriptor) {
              descriptorPromise->set_value(
                  std::make_tuple(error, std::move(descriptor)));
            },
            [sendPromise{std::move(sendPromise)}](
                const tensorpipe::Error& error) {
              sendPromise->set_value(error);
            },
            sendStream);

        Error descriptorError;
        TDescriptor descriptor;
        std::tie(descriptorError, descriptor) = descriptorFuture.get();

        EXPECT_FALSE(descriptorError) << descriptorError.what();
        peers_->send(PeerGroup::kClient, descriptor);
        Error sendError = sendFuture.get();
        EXPECT_FALSE(sendError) << sendError.what();
        TP_CUDA_CHECK(cudaFree(ptr));

        peers_->done(PeerGroup::kServer);
        peers_->join(PeerGroup::kServer);

        ctx->join();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        std::shared_ptr<Context> ctx = GetParam()->makeContext("client");
        auto channel = std::static_pointer_cast<cuda_ipc::Channel>(
            ctx->createChannel(std::move(conn), Endpoint::kConnect));

        TP_CUDA_CHECK(cudaSetDevice(0));
        cudaStream_t recvStream;
        TP_CUDA_CHECK(cudaStreamCreate(&recvStream));
        void* ptr;
        TP_CUDA_CHECK(cudaMalloc(&ptr, kSize));

        auto descriptor = peers_->recv(PeerGroup::kClient);

        // Perform recv and wait for completion.
        auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
        auto recvFuture = recvPromise->get_future();

        channel->recv(
            std::move(descriptor),
            ptr,
            kSize,
            [recvPromise{std::move(recvPromise)}](
                const tensorpipe::Error& error) {
              recvPromise->set_value(error);
            },
            recvStream);

        Error recvError = recvFuture.get();
        EXPECT_FALSE(recvError) << recvError.what();

        std::array<uint8_t, kSize> data;
        TP_CUDA_CHECK(cudaMemcpy(data.data(), ptr, kSize, cudaMemcpyDefault));
        // Validate contents of vector.
        for (auto i = 0; i < kSize; i++) {
          EXPECT_EQ(data[i], 0x42);
        }
        TP_CUDA_CHECK(cudaFree(ptr));

        peers_->done(PeerGroup::kClient);
        peers_->join(PeerGroup::kClient);

        ctx->join();
      });
}

INSTANTIATE_TEST_CASE_P(CudaIpc, ChannelTest, ::testing::Values(&helper));
INSTANTIATE_TEST_CASE_P(
    CudaIpc,
    CudaIpcChannelTest,
    ::testing::Values(&helper));
