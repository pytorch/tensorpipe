/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <sycl/sycl.hpp>
#include <tensorpipe/test/channel/channel_test_xpu.h>
#include <array>
#include <future>

using namespace tensorpipe;
using namespace tensorpipe::channel;

void slowKernel(sycl::queue& q, void* ptr, size_t bytes) {
  // Interpret pointer as uint8_t* and run some compute to take time.
  auto n = bytes;
  q.submit([&](sycl::handler& h) {
    uint8_t* dev = static_cast<uint8_t*>(ptr);
    h.parallel_for<class slow_kernel>(
        sycl::range<1>{(n + 255) / 256}, [=](sycl::id<1> id) {
          // a tiny busy-work loop (kernel-level). Keep simple to be portable.
          size_t base = id[0] * 256;
          for (size_t i = 0; i < 256; ++i) {
            if (base + i < n) {
              // do some dummy arithmetic to create latency
              dev[base + i] = (uint8_t)((dev[base + i] + 17) ^ 13);
            }
          }
        });
  });
}

class ReceiverWaitsForStartEventTest : public ClientServerChannelTestCase {
  static constexpr size_t kSize = 1024;

  void server(std::shared_ptr<Channel> channel) override {
    // Use device 0's queue
    sycl::queue& sendQueue = tensorpipe::xpu::getDefaultXPUQueue(0);

    // Allocate device memory
    void* ptr = sycl::malloc_device(kSize, sendQueue);

    // Delay sendQueue with computations on buffer.
    slowKernel(sendQueue, ptr, kSize);

    // Set buffer to target value (memset). DPC++ supports queue.memset.
    sendQueue.memset(ptr, 0x42, kSize).wait();

    // Perform send using channel, wrap in promise/future like CUDA test
    auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto sendFuture = sendPromise->get_future();

    XpuBuffer buf{.ptr = ptr, .queue = &sendQueue};
    channel->send(
        buf,
        kSize,
        [sendPromise{std::move(sendPromise)}](const tensorpipe::Error& error) {
          sendPromise->set_value(error);
        });

    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    sycl::free(ptr, sendQueue);

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    sycl::queue& recvQueue = tensorpipe::xpu::getDefaultXPUQueue(0);

    void* ptr = sycl::malloc_device(kSize, recvQueue);

    auto recvPromise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto recvFuture = recvPromise->get_future();

    XpuBuffer buf{.ptr = ptr, .queue = &recvQueue};
    channel->recv(
        buf,
        kSize,
        [recvPromise{std::move(recvPromise)}](const tensorpipe::Error& error) {
          recvPromise->set_value(error);
        });

    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // ensure recv work (and any copies) have completed on recvQueue
    recvQueue.wait();

    std::vector<uint8_t> data(kSize);
    // copy device -> host
    recvQueue.memcpy(data.data(), ptr, kSize).wait();

    for (auto v : data) {
      EXPECT_EQ(v, 0x42);
    }

    sycl::free(ptr, recvQueue);

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(XpuChannelTestSuite, ReceiverWaitsForStartEvent);

class SendOffsetAllocationTest : public ClientServerChannelTestCase {
 public:
  static constexpr int kDataSize = 256;
  static constexpr int kOffset = 128;

  void server(std::shared_ptr<Channel> channel) override {
    sycl::queue& q = tensorpipe::xpu::getDefaultXPUQueue(0);

    // allocate a larger region and use an offset inside it
    void* basePtr = sycl::malloc_device(kOffset + kDataSize, q);

    // Set the head region to 0xff and the data region to 0x42
    q.memset(basePtr, 0xff, kOffset).wait();
    q.memset(static_cast<uint8_t*>(basePtr) + kOffset, 0x42, kDataSize).wait();

    // build buffer pointing at offset
    XpuBuffer buf{.ptr = static_cast<uint8_t*>(basePtr) + kOffset, .queue = &q};

    // send and wait (using helper pattern, or do the promise/future)
    std::future<Error> sendFuture;
    {
      auto sendPromise = std::make_shared<std::promise<tensorpipe::Error>>();
      sendFuture = sendPromise->get_future();
      channel->send(
          buf,
          kDataSize,
          [sendPromise{std::move(sendPromise)}](
              const tensorpipe::Error& error) {
            sendPromise->set_value(error);
          });
    }

    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    sycl::free(basePtr, q);

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    // Use your helper to create a DataWrapper (or do manual allocate)
    std::unique_ptr<DataWrapper> wrappedData =
        helper_->makeDataWrapper(kDataSize);

    std::future<Error> recvFuture = recvWithFuture(channel, *wrappedData);
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents
    EXPECT_THAT(wrappedData->unwrap(), ::testing::Each(0x42));

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(XpuChannelTestSuite, SendOffsetAllocation);
