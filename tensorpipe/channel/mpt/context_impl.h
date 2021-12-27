/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/channel/mpt/nop_types.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create(
      std::vector<std::shared_ptr<transport::Context>> contexts,
      std::vector<std::shared_ptr<transport::Listener>> listeners);

  ContextImpl(
      std::vector<std::shared_ptr<transport::Context>> contexts,
      std::vector<std::shared_ptr<transport::Listener>> listeners,
      std::unordered_map<Device, std::string> deviceDescriptors);

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  // Implement the DeferredExecutor interface.
  bool inLoop() const override;
  void deferToLoop(std::function<void()> fn) override;

  using connection_request_callback_fn =
      std::function<void(const Error&, std::shared_ptr<transport::Connection>)>;

  const std::vector<std::string>& addresses() const;

  uint64_t registerConnectionRequest(
      uint64_t laneIdx,
      connection_request_callback_fn fn);

  void unregisterConnectionRequest(uint64_t registrationId);

  std::shared_ptr<transport::Connection> connect(
      uint64_t laneIdx,
      std::string address);

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void initImplFromLoop() override;
  void handleErrorImpl() override;
  void joinImpl() override;
  void setIdImpl() override;

 private:
  OnDemandDeferredExecutor loop_;

  void acceptLane(uint64_t laneIdx);
  void onAcceptOfLane(std::shared_ptr<transport::Connection> connection);
  void onReadClientHelloOnLane(
      std::shared_ptr<transport::Connection> connection,
      const Packet& nopPacketIn);

  const std::vector<std::shared_ptr<transport::Context>> contexts_;
  const std::vector<std::shared_ptr<transport::Listener>> listeners_;

  uint64_t numLanes_{0};
  std::vector<std::string> addresses_;

  uint64_t nextConnectionRequestRegistrationId_{0};

  // Needed to keep them alive.
  std::unordered_set<std::shared_ptr<transport::Connection>>
      connectionsWaitingForHello_;

  std::unordered_map<uint64_t, connection_request_callback_fn>
      connectionRequestRegistrations_;
};

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
