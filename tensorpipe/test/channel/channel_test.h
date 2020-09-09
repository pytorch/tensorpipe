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

class DataWrapper {
 public:
  virtual ~DataWrapper() = default;
  virtual void* data() = 0;
  virtual size_t size() = 0;
  virtual void wrap(const void* ptr) = 0;
  virtual void unwrap(void* ptr) = 0;
};

class IdWrapper : public DataWrapper {
 public:
  explicit IdWrapper(size_t len) : size_(len) {}

  void* data() override {
    return data_;
  }

  size_t size() override {
    return size_;
  }

  void wrap(const void* ptr) override {
    data_ = const_cast<void*>(ptr);
  }

  void unwrap(void* ptr) override {
    ASSERT_EQ(data_, ptr);
  }

 private:
  void* data_;
  size_t size_;
};

class ChannelTestHelper {
 public:
  virtual ~ChannelTestHelper() = default;

  // FIXME: This is needed for a workaround to avoid running a generic test
  // against CUDA channels. It should be removed once the channel (and test)
  // hierarchies are separated.
  virtual std::string channelName() = 0;

  virtual std::shared_ptr<tensorpipe::channel::CpuContext> makeContext(
      std::string id) = 0;

  virtual std::shared_ptr<PeerGroup> makePeerGroup() {
    return std::make_shared<ThreadPeerGroup>();
  }

  virtual std::shared_ptr<DataWrapper> makeBuffer(size_t len) {
    return std::make_shared<IdWrapper>(len);
  }
};

class ChannelTest : public ::testing::TestWithParam<ChannelTestHelper*> {
 protected:
  ChannelTestHelper* helper_;
  std::shared_ptr<PeerGroup> peers_;

 public:
  ChannelTest() : helper_(GetParam()), peers_(helper_->makePeerGroup()) {}

  void testConnection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          server,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          client) {
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

  [[nodiscard]] std::pair<
      std::future<
          std::tuple<tensorpipe::Error, tensorpipe::channel::TDescriptor>>,
      std::future<tensorpipe::Error>>
  sendWithFuture(
      std::shared_ptr<tensorpipe::channel::CpuChannel> channel,
      const tensorpipe::CpuTensor& tensor) {
    auto descriptorPromise = std::make_shared<
        std::promise<std::tuple<tensorpipe::Error, std::string>>>();
    auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto descriptorFuture = descriptorPromise->get_future();
    auto future = promise->get_future();

    channel->send(
        tensor,
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

  [[nodiscard]] std::future<tensorpipe::Error> recvWithFuture(
      std::shared_ptr<tensorpipe::channel::CpuChannel> channel,
      tensorpipe::channel::TDescriptor descriptor,
      const tensorpipe::CpuTensor& tensor) {
    auto promise = std::make_shared<std::promise<tensorpipe::Error>>();
    auto future = promise->get_future();

    channel->recv(
        std::move(descriptor),
        tensor,
        [promise{std::move(promise)}](const tensorpipe::Error& error) {
          promise->set_value(error);
        });
    return future;
  }
};
