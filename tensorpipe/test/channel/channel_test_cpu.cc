/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/channel_test_cpu.h>

#include <numeric>

#include <tensorpipe/test/channel/channel_test.h>

using namespace tensorpipe;
using namespace tensorpipe::channel;

// Call send and recv with a null pointer and a length of 0.
class NullPointerTest : public ClientServerChannelTestCase {
  void server(std::shared_ptr<Channel> channel) override {
    // Perform send and wait for completion.
    std::future<Error> sendFuture =
        sendWithFuture(channel, CpuBuffer{.ptr = nullptr}, 0);
    Error sendError = sendFuture.get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    // Perform recv and wait for completion.
    std::future<Error> recvFuture =
        recvWithFuture(channel, CpuBuffer{.ptr = nullptr}, 0);
    Error recvError = recvFuture.get();
    EXPECT_FALSE(recvError) << recvError.what();

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(CpuChannelTestSuite, NullPointer);

// This test wants to make sure that the "heavy lifting" of copying data isn't
// performed inline inside the recv method as that would make the user-facing
// read method of the pipe blocking.
// However, since we can't really check that behavior, we'll check a highly
// correlated one: that the recv callback isn't called inline from within the
// recv method. We do so by having that behavior cause a deadlock.
class CallbacksAreDeferredTest : public ClientServerChannelTestCase {
  static constexpr auto kDataSize = 256;

 public:
  void server(std::shared_ptr<Channel> channel) override {
    // Initialize with sequential values.
    std::vector<uint8_t> data(kDataSize);
    std::iota(data.begin(), data.end(), 0);

    // Perform send and wait for completion.
    std::promise<Error> sendPromise;
    auto mutex = std::make_shared<std::mutex>();
    std::unique_lock<std::mutex> callerLock(*mutex);
    channel->send(
        CpuBuffer{.ptr = data.data()},
        kDataSize,
        [&sendPromise, mutex](const Error& error) {
          std::unique_lock<std::mutex> calleeLock(*mutex);
          sendPromise.set_value(error);
        });
    callerLock.unlock();
    Error sendError = sendPromise.get_future().get();
    EXPECT_FALSE(sendError) << sendError.what();

    this->peers_->done(PeerGroup::kServer);
    this->peers_->join(PeerGroup::kServer);
  }

  void client(std::shared_ptr<Channel> channel) override {
    // Initialize with zeroes.
    std::vector<uint8_t> data(kDataSize);
    std::fill(data.begin(), data.end(), 0);

    // Perform recv and wait for completion.
    std::promise<Error> recvPromise;
    std::mutex mutex;
    std::unique_lock<std::mutex> callerLock(mutex);
    channel->recv(
        CpuBuffer{.ptr = data.data()},
        kDataSize,
        [&recvPromise, &mutex](const Error& error) {
          std::unique_lock<std::mutex> calleeLock(mutex);
          recvPromise.set_value(error);
        });
    callerLock.unlock();
    Error recvError = recvPromise.get_future().get();
    EXPECT_FALSE(recvError) << recvError.what();

    // Validate contents of vector.
    for (auto i = 0; i < kDataSize; i++) {
      EXPECT_EQ(data[i], i);
    }

    this->peers_->done(PeerGroup::kClient);
    this->peers_->join(PeerGroup::kClient);
  }
};

CHANNEL_TEST(CpuChannelTestSuite, CallbacksAreDeferred);
