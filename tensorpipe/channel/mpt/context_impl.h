/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <tensorpipe/channel/cpu_context.h>
#include <tensorpipe/channel/mpt/nop_types.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create(
      std::vector<std::shared_ptr<transport::Context>> contexts,
      std::vector<std::shared_ptr<transport::Listener>> listeners);

  ContextImpl(
      std::vector<std::shared_ptr<transport::Context>> contexts,
      std::vector<std::shared_ptr<transport::Listener>> listeners);

  void init();

  std::shared_ptr<CpuChannel> createChannel(
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
  void closeImpl() override;
  void joinImpl() override;
  void setIdImpl() override;

 private:
  OnDemandDeferredExecutor loop_;
  Error error_{Error::kSuccess};

  void initFromLoop();

  void registerConnectionRequestFromLoop(
      uint64_t laneIdx,
      uint64_t registrationId,
      connection_request_callback_fn fn);

  void unregisterConnectionRequestFromLoop(uint64_t registrationId);

  void acceptLane(uint64_t laneIdx);
  void onAcceptOfLane(std::shared_ptr<transport::Connection> connection);
  void onReadClientHelloOnLane(
      std::shared_ptr<transport::Connection> connection,
      const Packet& nopPacketIn);

  void setError(Error error);
  void handleError();
  void closeFromLoop();

  const std::vector<std::shared_ptr<transport::Context>> contexts_;
  const std::vector<std::shared_ptr<transport::Listener>> listeners_;

  uint64_t numLanes_{0};
  std::vector<std::string> addresses_;

  // This is atomic because it may be accessed from outside the loop.
  std::atomic<uint64_t> nextConnectionRequestRegistrationId_{0};

  // Needed to keep them alive.
  std::unordered_set<std::shared_ptr<transport::Connection>>
      connectionsWaitingForHello_;

  std::unordered_map<uint64_t, connection_request_callback_fn>
      connectionRequestRegistrations_;

  LazyCallbackWrapper<ContextImpl> lazyCallbackWrapper_{*this, this->loop_};
  EagerCallbackWrapper<ContextImpl> eagerCallbackWrapper_{*this, this->loop_};

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::LazyCallbackWrapper;
  template <typename T>
  friend class tensorpipe::EagerCallbackWrapper;
};

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
