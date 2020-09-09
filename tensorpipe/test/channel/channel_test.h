/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/tensor.h>
#include <tensorpipe/test/peer_group.h>
#include <tensorpipe/transport/uv/context.h>

template <typename TTensor>
class DataWrapper {
  // Non-copyable.
  DataWrapper(const DataWrapper&) = delete;
  DataWrapper& operator=(const DataWrapper&) = delete;
  // Non-movable.
  DataWrapper(const DataWrapper&&) = delete;
  DataWrapper& operator=(const DataWrapper&&) = delete;
};

template <>
class DataWrapper<tensorpipe::CpuTensor> {
 public:
  explicit DataWrapper(size_t length) : vector_(length) {}

  explicit DataWrapper(const std::vector<uint8_t>& v) : vector_(v) {}

  tensorpipe::CpuTensor tensor() const {
    return tensorpipe::CpuTensor{
        .ptr = const_cast<uint8_t*>(vector_.data()),
        .length = vector_.size(),
    };
  }

  std::vector<uint8_t> unwrap() {
    return vector_;
  }

 private:
  std::vector<uint8_t> vector_;
};

#if TENSORPIPE_HAS_CUDA

template <>
class DataWrapper<tensorpipe::CudaTensor> {
 public:
  explicit DataWrapper(size_t length) : length_(length) {
    if (length_ > 0) {
      EXPECT_EQ(cudaSuccess, cudaSetDevice(0));
      EXPECT_EQ(cudaSuccess, cudaStreamCreate(&stream_));
      EXPECT_EQ(cudaSuccess, cudaMalloc(&cudaPtr_, length_));
    }
  }

  explicit DataWrapper(const std::vector<uint8_t>& v) : DataWrapper(v.size()) {
    if (length_ > 0) {
      EXPECT_EQ(
          cudaSuccess,
          cudaMemcpy(cudaPtr_, v.data(), length_, cudaMemcpyDefault));
    }
  }

  tensorpipe::CudaTensor tensor() const {
    return tensorpipe::CudaTensor{
        .ptr = cudaPtr_,
        .length = length_,
        .stream = stream_,
    };
  }

  std::vector<uint8_t> unwrap() {
    std::vector<uint8_t> v(length_);
    if (length_ > 0) {
      EXPECT_EQ(
          cudaSuccess,
          cudaMemcpy(v.data(), cudaPtr_, length_, cudaMemcpyDefault));
    }

    return v;
  }

  ~DataWrapper() {
    if (length_ > 0) {
      EXPECT_EQ(cudaSuccess, cudaFree(cudaPtr_));
    }
  }

 private:
  void* cudaPtr_{nullptr};
  size_t length_{0};
  cudaStream_t stream_{cudaStreamDefault};
};

#endif // TENSORPIPE_HAS_CUDA

template <typename TTensor>
class ChannelTestHelper {
 public:
  virtual ~ChannelTestHelper() = default;

  virtual std::shared_ptr<tensorpipe::channel::Context<TTensor>> makeContext(
      std::string id) = 0;

  virtual std::shared_ptr<PeerGroup> makePeerGroup() {
    return std::make_shared<ThreadPeerGroup>();
  }
};

template <typename TTensor>
[[nodiscard]] std::pair<
    std::future<
        std::tuple<tensorpipe::Error, tensorpipe::channel::TDescriptor>>,
    std::future<tensorpipe::Error>>
sendWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel<TTensor>> channel,
    TTensor tensor) {
  auto descriptorPromise = std::make_shared<
      std::promise<std::tuple<tensorpipe::Error, std::string>>>();
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto descriptorFuture = descriptorPromise->get_future();
  auto future = promise->get_future();

  channel->send(
      std::move(tensor),
      [descriptorPromise{std::move(descriptorPromise)}](
          const tensorpipe::Error& error, std::string descriptor) {
        descriptorPromise->set_value(
            std::make_tuple(error, std::move(descriptor)));
      },
      [promise{std::move(promise)}](const tensorpipe::Error& error) {
        promise->set_value(error);
      });
  return {std::move(descriptorFuture), std::move(future)};
}

template <typename TTensor>
[[nodiscard]] std::future<tensorpipe::Error> recvWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel<TTensor>> channel,
    tensorpipe::channel::TDescriptor descriptor,
    TTensor tensor) {
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = promise->get_future();

  channel->recv(
      std::move(descriptor),
      std::move(tensor),
      [promise{std::move(promise)}](const tensorpipe::Error& error) {
        promise->set_value(error);
      });
  return future;
}

template <typename TTensor>
class ChannelTest {
 public:
  virtual void run(ChannelTestHelper<TTensor>* helper) {
    helper_ = helper;
    peers_ = helper_->makePeerGroup();

    auto addr = "127.0.0.1";

    peers_->spawn(
        [&] {
          auto context = std::make_shared<tensorpipe::transport::uv::Context>();
          context->setId("server_harness");

          auto listener = context->listen(addr);

          std::promise<std::shared_ptr<tensorpipe::transport::Connection>>
              connectionProm;
          listener->accept(
              [&](const tensorpipe::Error& error,
                  std::shared_ptr<tensorpipe::transport::Connection>
                      connection) {
                ASSERT_FALSE(error) << error.what();
                connectionProm.set_value(std::move(connection));
              });

          peers_->send(PeerGroup::kClient, listener->addr());
          server(connectionProm.get_future().get());

          context->join();
        },
        [&] {
          auto context = std::make_shared<tensorpipe::transport::uv::Context>();
          context->setId("client_harness");

          auto laddr = peers_->recv(PeerGroup::kClient);
          client(context->connect(laddr));

          context->join();
        });
  }

  virtual void server(std::shared_ptr<tensorpipe::transport::Connection>){};
  virtual void client(std::shared_ptr<tensorpipe::transport::Connection>){};

 protected:
  ChannelTestHelper<TTensor>* helper_;
  std::shared_ptr<PeerGroup> peers_;
};

class CpuChannelTest : public ::testing::TestWithParam<
                           ChannelTestHelper<tensorpipe::CpuTensor>*> {};

#if TENSORPIPE_HAS_CUDA
class CudaChannelTest : public ::testing::TestWithParam<
                            ChannelTestHelper<tensorpipe::CudaTensor>*> {};
#endif // TENSORPIPE_HAS_CUDA

#define CHANNEL_TEST(type, name)    \
  TEST_P(type##ChannelTest, name) { \
    name##Test t;                   \
    t.run(GetParam());              \
  }

#define _CHANNEL_TEST(type, name)           \
  TEST_P(type##ChannelTest, name) {         \
    name##Test<tensorpipe::type##Tensor> t; \
    t.run(GetParam());                      \
  }

#define _CHANNEL_TEST_CPU(name) _CHANNEL_TEST(Cpu, name)

#if TENSORPIPE_HAS_CUDA
#define _CHANNEL_TEST_CUDA(name) _CHANNEL_TEST(Cuda, name)
#else // TENSORPIPE_HAS_CUDA
#define _CHANNEL_TEST_CUDA(name)
#endif // TENSORPIPE_HAS_CUDA

#define CHANNEL_TEST_GENERIC(name) \
  _CHANNEL_TEST_CPU(name);         \
  _CHANNEL_TEST_CUDA(name);
