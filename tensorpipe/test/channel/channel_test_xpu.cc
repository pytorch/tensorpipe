/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
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
