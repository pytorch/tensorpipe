/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/context.h>
#include <tensorpipe/channel/mpt/factory.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/test/channel/channel_test_cpu.h>
#include <tensorpipe/transport/connection.h>

namespace {

class MptChannelTestHelper : public CpuChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    std::vector<std::shared_ptr<tensorpipe::transport::Context>> contexts = {
        tensorpipe::transport::uv::create(),
        tensorpipe::transport::uv::create(),
        tensorpipe::transport::uv::create()};
    std::vector<std::shared_ptr<tensorpipe::transport::Listener>> listeners = {
        contexts[0]->listen("127.0.0.1"),
        contexts[1]->listen("127.0.0.1"),
        contexts[2]->listen("127.0.0.1")};
    auto context = tensorpipe::channel::mpt::create(
        std::move(contexts), std::move(listeners));
    context->setId(std::move(id));
    return context;
  }
};

MptChannelTestHelper helper;

class MptChannelTestSuite : public ChannelTestSuite {};

} // namespace

class ContextIsNotJoinedTest : public ChannelTestCase {
  // Because it's static we must define it out-of-line (until C++-17, where we
  // can mark this inline).
  static const std::string kReady;

 public:
  void run(ChannelTestHelper* helper) override {
    auto addr = "127.0.0.1";

    helper_ = helper;
    peers_ = helper_->makePeerGroup();
    peers_->spawn(
        [&] {
          auto context = tensorpipe::transport::uv::create();
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
          auto context = tensorpipe::transport::uv::create();
          context->setId("client_harness");

          auto laddr = peers_->recv(PeerGroup::kClient);
          client(context->connect(laddr));

          context->join();
        });
  }

  void server(std::shared_ptr<tensorpipe::transport::Connection> conn) {
    std::shared_ptr<tensorpipe::channel::Context> context =
        this->helper_->makeContext("server");
    this->peers_->send(PeerGroup::kClient, kReady);
    context->createChannel(
        {std::move(conn)}, tensorpipe::channel::Endpoint::kListen);
  }

  void client(std::shared_ptr<tensorpipe::transport::Connection> conn) {
    std::shared_ptr<tensorpipe::channel::Context> context =
        this->helper_->makeContext("client");
    EXPECT_EQ(kReady, this->peers_->recv(PeerGroup::kClient));
    context->createChannel(
        {std::move(conn)}, tensorpipe::channel::Endpoint::kConnect);
  }

 protected:
  ChannelTestHelper* helper_;
  std::shared_ptr<PeerGroup> peers_;
};

const std::string ContextIsNotJoinedTest::kReady = "ready";

CHANNEL_TEST(MptChannelTestSuite, ContextIsNotJoined);

INSTANTIATE_TEST_CASE_P(Mpt, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(Mpt, CpuChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(Mpt, MptChannelTestSuite, ::testing::Values(&helper));
