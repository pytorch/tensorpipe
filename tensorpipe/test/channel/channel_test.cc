/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/channel_test.h>

#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <unordered_map>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>

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
class TestChannel : public channel::Channel,
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
        [callback{std::move(callback)}, vec](const Error& error) {
          callback(error);
        });
  }

 private:
  void init_() {
    nextRead_();
  }

  void nextRead_() {
    connection_->read(runIfAlive(
        *this,
        std::function<void(TestChannel&, const Error&, const void*, size_t)>(
            [](TestChannel& channel,
               const Error& error,
               const void* ptr,
               size_t len) { channel.onDoneMessage_(error, ptr, len); })));
  }

  void onDoneMessage_(const Error& error, const void* ptr, size_t len) {
    TSendCallback callback;

    {
      std::unique_lock<std::mutex> lock(mutex_);
      callback = std::move(sendCallbacks_.front());
      sendCallbacks_.pop_front();
    }

    if (error) {
      callback(error);
      return;
    }

    bufferToType<DoneMessage>(ptr, len);
    callback(Error::kSuccess);
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

class TestChannelFactory : public channel::ChannelFactory {
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

  std::shared_ptr<channel::Channel> createChannel(
      std::shared_ptr<transport::Connection> conn) override {
    auto channel = std::make_shared<TestChannel>(std::move(conn));
    channel->init_();
    return channel;
  }

 private:
  std::string domainDescriptor_;
};

} // namespace

TEST(TestChannelFactoryTest, Name) {
  std::shared_ptr<channel::ChannelFactory> factory =
      std::make_shared<TestChannelFactory>();
  EXPECT_EQ(factory->name(), "TestChannelFactory");
}

INSTANTIATE_TYPED_TEST_CASE_P(Test, ChannelFactoryTest, TestChannelFactory);
