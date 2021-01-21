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
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/config.h>
#include <tensorpipe/test/peer_group.h>
#include <tensorpipe/transport/listener.h>
#include <tensorpipe/transport/uv/context.h>

#if TENSORPIPE_SUPPORTS_CUDA
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#endif

template <typename TBuffer>
class DataWrapper {};

template <>
class DataWrapper<tensorpipe::CpuBuffer> {
 public:
  explicit DataWrapper(size_t length) : vector_(length) {}

  explicit DataWrapper(std::vector<uint8_t> v) : vector_(v) {}

  tensorpipe::CpuBuffer buffer() const {
    return tensorpipe::CpuBuffer{
        const_cast<uint8_t*>(vector_.data()), vector_.size()};
  }

  const std::vector<uint8_t>& unwrap() {
    return vector_;
  }

 private:
  std::vector<uint8_t> vector_;
};

#if TENSORPIPE_SUPPORTS_CUDA

template <>
class DataWrapper<tensorpipe::CudaBuffer> {
 public:
  // Non-copyable.
  DataWrapper(const DataWrapper&) = delete;
  DataWrapper& operator=(const DataWrapper&) = delete;

  explicit DataWrapper(size_t length) : length_(length) {
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaSetDevice(0));
      TP_CUDA_CHECK(cudaStreamCreate(&stream_));
      TP_CUDA_CHECK(cudaMalloc(&cudaPtr_, length_));
    }
  }

  explicit DataWrapper(std::vector<uint8_t> v) : DataWrapper(v.size()) {
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaMemcpy(cudaPtr_, v.data(), length_, cudaMemcpyDefault));
    }
  }

  explicit DataWrapper(DataWrapper&& other) noexcept
      : cudaPtr_(other.cudaPtr_),
        length_(other.length_),
        stream_(other.stream_) {
    other.cudaPtr_ = nullptr;
    other.length_ = 0;
    other.stream_ = cudaStreamDefault;
  }

  DataWrapper& operator=(DataWrapper&& other) {
    std::swap(cudaPtr_, other.cudaPtr_);
    std::swap(length_, other.length_);
    std::swap(stream_, other.stream_);

    return *this;
  }

  tensorpipe::CudaBuffer buffer() const {
    return tensorpipe::CudaBuffer{cudaPtr_, length_, stream_};
  }

  std::vector<uint8_t> unwrap() {
    std::vector<uint8_t> v(length_);
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaMemcpy(v.data(), cudaPtr_, length_, cudaMemcpyDefault));
    }

    return v;
  }

  ~DataWrapper() {
    if (length_ > 0) {
      TP_CUDA_CHECK(cudaFree(cudaPtr_));
      TP_CUDA_CHECK(cudaStreamDestroy(stream_));
    }
  }

 private:
  void* cudaPtr_{nullptr};
  size_t length_{0};
  cudaStream_t stream_{cudaStreamDefault};
};

#endif // TENSORPIPE_SUPPORTS_CUDA

template <typename TBuffer>
class ChannelTestHelper {
 public:
  virtual ~ChannelTestHelper() = default;

  std::shared_ptr<tensorpipe::channel::Context<TBuffer>> makeContext(
      std::string id,
      bool skipViabilityCheck = false) {
    std::shared_ptr<tensorpipe::channel::Context<TBuffer>> ctx =
        makeContextInternal(std::move(id));
    if (!skipViabilityCheck) {
      EXPECT_TRUE(ctx->isViable());
    }
    return ctx;
  }

  virtual std::shared_ptr<PeerGroup> makePeerGroup() {
    return std::make_shared<ThreadPeerGroup>();
  }

 protected:
  virtual std::shared_ptr<tensorpipe::channel::Context<TBuffer>>
  makeContextInternal(std::string id) = 0;
};

template <typename TBuffer>
[[nodiscard]] std::pair<
    std::future<
        std::tuple<tensorpipe::Error, tensorpipe::channel::TDescriptor>>,
    std::future<tensorpipe::Error>>
sendWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel<TBuffer>> channel,
    TBuffer buffer) {
  auto descriptorPromise = std::make_shared<
      std::promise<std::tuple<tensorpipe::Error, std::string>>>();
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto descriptorFuture = descriptorPromise->get_future();
  auto future = promise->get_future();

  channel->send(
      buffer,
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

template <typename TBuffer>
[[nodiscard]] std::future<tensorpipe::Error> recvWithFuture(
    std::shared_ptr<tensorpipe::channel::Channel<TBuffer>> channel,
    tensorpipe::channel::TDescriptor descriptor,
    TBuffer buffer) {
  auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
  auto future = promise->get_future();

  channel->recv(
      std::move(descriptor),
      buffer,
      [promise{std::move(promise)}](const tensorpipe::Error& error) {
        promise->set_value(error);
      });
  return future;
}

template <typename TBuffer>
class ChannelTestCase {
 public:
  virtual void run(ChannelTestHelper<TBuffer>* helper) = 0;

  virtual ~ChannelTestCase() = default;
};

template <typename TBuffer>
class ClientServerChannelTestCase : public ChannelTestCase<TBuffer> {
 public:
  void run(ChannelTestHelper<TBuffer>* helper) override {
    auto addr = "127.0.0.1";

    helper_ = helper;
    peers_ = helper_->makePeerGroup();
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

  virtual void server(
      std::shared_ptr<tensorpipe::transport::Connection> connection) {}
  virtual void client(
      std::shared_ptr<tensorpipe::transport::Connection> connection) {}

 protected:
  ChannelTestHelper<TBuffer>* helper_;
  std::shared_ptr<PeerGroup> peers_;
};

class CpuChannelTestSuite : public ::testing::TestWithParam<
                                ChannelTestHelper<tensorpipe::CpuBuffer>*> {};

#if TENSORPIPE_SUPPORTS_CUDA
class CudaChannelTestSuite : public ::testing::TestWithParam<
                                 ChannelTestHelper<tensorpipe::CudaBuffer>*> {};
#endif // TENSORPIPE_SUPPORTS_CUDA

#define _CHANNEL_TEST(type, suite, name)    \
  TEST_P(suite, name) {                     \
    name##Test<tensorpipe::type##Buffer> t; \
    t.run(GetParam());                      \
  }

#define _CHANNEL_TEST_CPU(name) _CHANNEL_TEST(Cpu, CpuChannelTestSuite, name)

#if TENSORPIPE_SUPPORTS_CUDA
#define _CHANNEL_TEST_CUDA(name) _CHANNEL_TEST(Cuda, CudaChannelTestSuite, name)
#else // TENSORPIPE_SUPPORTS_CUDA
#define _CHANNEL_TEST_CUDA(name)
#endif // TENSORPIPE_SUPPORTS_CUDA

// Register a generic (template) channel test.
#define CHANNEL_TEST_GENERIC(name) \
  _CHANNEL_TEST_CPU(name);         \
  _CHANNEL_TEST_CUDA(name);

// Register a (non-template) channel test.
#define CHANNEL_TEST(suite, name) \
  TEST_P(suite, name) {           \
    name##Test t;                 \
    t.run(GetParam());            \
  }
