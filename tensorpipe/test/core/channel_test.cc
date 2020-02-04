/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <thread>
#include <unordered_map>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/core/channel.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/uv/context.h>

#include <gtest/gtest.h>

using namespace tensorpipe;

namespace {

template <
    typename T,
    typename std::enable_if<std::is_trivially_copyable<T>::value, bool>::type =
        false>
T bufferToType(const void* ptr, size_t len) {
  T ret;
  TP_DCHECK_EQ(len, sizeof(ret));
  std::memcpy(&ret, ptr, len);
  return ret;
}

template <
    typename T,
    typename std::enable_if<std::is_trivially_copyable<T>::value, bool>::type =
        false>
T bufferToType(std::vector<uint8_t> buf) {
  T ret;
  TP_DCHECK_EQ(buf.size(), sizeof(ret));
  std::memcpy(&ret, buf.data(), buf.size());
  return ret;
}

template <
    typename T,
    typename std::enable_if<std::is_trivially_copyable<T>::value, bool>::type =
        false>
std::vector<uint8_t> typeToBuffer(const T& t) {
  std::vector<uint8_t> ret(sizeof(t));
  std::memcpy(ret.data(), &t, sizeof(t));
  return ret;
}

// Forward declaration.
class TestChannelFactory;

// Implementation of tensorpipe::Channel for test purposes.
class TestChannel : public Channel,
                    public std::enable_shared_from_this<TestChannel> {
  // Store pointer and length in descriptor.
  // This test runs in a single process so we can use memcpy directly.
  struct Descriptor {
    const void* ptr{nullptr};
    size_t length{0};
  };

  // Message sent by receiver when recv has completed.
  struct DoneMessage {
    bool done{true};
  };

 public:
  explicit TestChannel(std::shared_ptr<transport::Connection> connection)
      : connection_(std::move(connection)) {}

  TDescriptor send(const void* ptr, size_t length, TSendCallback callback)
      override {
    std::unique_lock<std::mutex> lock(mutex_);
    sendCallbacks_.emplace_back(std::move(callback));
    return typeToBuffer(Descriptor{ptr, length});
  }

  void recv(
      TDescriptor descriptorBuffer,
      void* ptr,
      size_t length,
      TRecvCallback callback) override {
    const auto descriptor = bufferToType<Descriptor>(descriptorBuffer);
    TP_DCHECK_EQ(length, descriptor.length);
    std::memcpy(ptr, descriptor.ptr, length);

    // Send message to peer that the recv is done.
    auto vec = std::make_shared<std::vector<uint8_t>>();
    *vec = typeToBuffer(DoneMessage());
    connection_->write(
        vec->data(),
        vec->size(),
        [callback{std::move(callback)}](const transport::Error& error) {
          TP_LOG_WARNING_IF(error) << error.what();
          callback();
        });
  }

 private:
  void init_() {
    nextRead_();
  }

  void nextRead_() {
    connection_->read(runIfAlive(
        *this,
        std::function<void(
            TestChannel&, const transport::Error&, const void*, size_t)>(
            [](TestChannel& channel,
               const transport::Error& error,
               const void* ptr,
               size_t len) {
              TP_LOG_WARNING_IF(error) << error.what();
              channel.onDoneMessage_(bufferToType<DoneMessage>(ptr, len));
            })));
  }

  void onDoneMessage_(DoneMessage /* unused */) {
    TSendCallback callback;

    {
      std::unique_lock<std::mutex> lock(mutex_);
      callback = std::move(sendCallbacks_.front());
      sendCallbacks_.pop_front();
    }

    callback();
    nextRead_();
  }

  friend class TestChannelFactory;

 private:
  std::mutex mutex_;

  // Control connection for this channel.
  // Used only to send acknowledgment of receipt to sender in this test.
  std::shared_ptr<transport::Connection> connection_;

  // Keep ordered list of send callbacks.
  // This assumes that send and recv are called in the same order.
  std::deque<TSendCallback> sendCallbacks_;
};

class TestChannelFactory : public ChannelFactory {
 public:
  TestChannelFactory() : ChannelFactory("TestChannelFactory") {
    std::ostringstream oss;
    oss << name() << ":" << getpid();
    domainDescriptor_ = oss.str();
  }

  ~TestChannelFactory() override {}

  const std::string& domainDescriptor() const override {
    return domainDescriptor_;
  }

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection> conn) override {
    auto channel = std::make_shared<TestChannel>(std::move(conn));
    channel->init_();
    return channel;
  }

 private:
  std::string domainDescriptor_;
};

void testConnectionPair(
    std::function<void(std::shared_ptr<transport::Connection>)> f1,
    std::function<void(std::shared_ptr<transport::Connection>)> f2) {
  auto context = std::make_shared<transport::uv::Context>();
  auto addr = "::1";

  {
    Queue<std::shared_ptr<transport::Connection>> q1, q2;

    // Listening side.
    auto listener = context->listen(addr);
    listener->accept([&](const transport::Error& error,
                         std::shared_ptr<transport::Connection> connection) {
      ASSERT_FALSE(error) << error.what();
      q1.push(std::move(connection));
    });

    // Connecting side.
    q2.push(context->connect(listener->addr()));

    // Run user specified functions.
    std::thread t1([&] { f1(q1.pop()); });
    std::thread t2([&] { f2(q2.pop()); });
    t1.join();
    t2.join();
  }

  context->join();
}

} // namespace

TEST(ChannelFactory, Name) {
  std::shared_ptr<ChannelFactory> factory =
      std::make_shared<TestChannelFactory>();
  EXPECT_EQ(factory->name(), "TestChannelFactory");
}

TEST(ChannelFactory, DomainDescriptor) {
  std::shared_ptr<ChannelFactory> factory1 =
      std::make_shared<TestChannelFactory>();
  std::shared_ptr<ChannelFactory> factory2 =
      std::make_shared<TestChannelFactory>();
  EXPECT_FALSE(factory1->domainDescriptor().empty());
  EXPECT_FALSE(factory2->domainDescriptor().empty());
  EXPECT_EQ(factory1->domainDescriptor(), factory2->domainDescriptor());
}

TEST(ChannelFactory, CreateChannel) {
  std::shared_ptr<ChannelFactory> factory1 =
      std::make_shared<TestChannelFactory>();
  std::shared_ptr<ChannelFactory> factory2 =
      std::make_shared<TestChannelFactory>();
  constexpr auto dataSize = 256;
  Queue<Channel::TDescriptor> descriptorQueue;

  testConnectionPair(
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory1->createChannel(std::move(conn));

        // Initialize with sequential values.
        std::vector<uint8_t> data(256);
        for (auto i = 0; i < data.size(); i++) {
          data[i] = i;
        }

        // Perform send and wait for completion.
        Channel::TDescriptor descriptor;
        std::future<void> future;
        std::tie(descriptor, future) = channel->send(data.data(), data.size());
        descriptorQueue.push(std::move(descriptor));
        future.wait();
      },
      [&](std::shared_ptr<transport::Connection> conn) {
        auto channel = factory2->createChannel(std::move(conn));

        // Initialize with zeroes.
        std::vector<uint8_t> data(256);
        for (auto i = 0; i < data.size(); i++) {
          data[i] = 0;
        }

        // Perform recv and wait for completion.
        channel->recv(descriptorQueue.pop(), data.data(), data.size()).wait();

        // Validate contents of vector.
        for (auto i = 0; i < data.size(); i++) {
          EXPECT_EQ(data[i], i);
        }
      });
}
