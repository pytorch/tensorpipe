/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/context.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {

std::shared_ptr<Context> Context::create() {
  return std::make_shared<Context>(ConstructorToken());
}

Context::Context(ConstructorToken /* unused */) {}

void Context::registerTransport(
    int64_t priority,
    std::string transport,
    std::shared_ptr<transport::Context> context) {
  TP_THROW_ASSERT_IF(transport.empty());
  TP_THROW_ASSERT_IF(contexts_.find(transport) != contexts_.end())
      << "transport " << transport << " already registered";
  TP_THROW_ASSERT_IF(
      contextsByPriority_.find(priority) != contextsByPriority_.end())
      << "transport with priority " << priority << " already registered";
  contexts_.emplace(transport, context);
  contextsByPriority_.emplace(priority, std::make_tuple(transport, context));
}

void Context::registerChannelFactory(
    int64_t priority,
    std::string name,
    std::shared_ptr<channel::ChannelFactory> channelFactory) {
  TP_THROW_ASSERT_IF(name.empty());
  TP_THROW_ASSERT_IF(channelFactories_.find(name) != channelFactories_.end())
      << "channel factory " << name << " already registered";
  TP_THROW_ASSERT_IF(
      channelFactoriesByPriority_.find(priority) !=
      channelFactoriesByPriority_.end())
      << "channel factory with priority " << priority << " already registered";
  channelFactories_.emplace(name, channelFactory);
  channelFactoriesByPriority_.emplace(
      priority, std::make_tuple(name, channelFactory));
}

std::shared_ptr<transport::Context> Context::getContextForTransport_(
    std::string transport) {
  auto iter = contexts_.find(transport);
  if (iter == contexts_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  return iter->second;
}

std::shared_ptr<channel::ChannelFactory> Context::getChannelFactory_(
    std::string name) {
  auto iter = channelFactories_.find(name);
  if (iter == channelFactories_.end()) {
    TP_THROW_EINVAL() << "unsupported channel factory " << name;
  }
  return iter->second;
}

void Context::close() {
  bool wasClosed = false;
  if (closed_.compare_exchange_strong(wasClosed, true)) {
    TP_DCHECK(!wasClosed);

    closingEmitter_.close();

    for (auto& iter : contexts_) {
      iter.second->close();
    }
    for (auto& iter : channelFactories_) {
      iter.second->close();
    }
  }
}

void Context::join() {
  close();

  bool wasJoined = false;
  if (joined_.compare_exchange_strong(wasJoined, true)) {
    TP_DCHECK(!wasJoined);

    for (auto& iter : contexts_) {
      iter.second->join();
    }
    for (auto& iter : channelFactories_) {
      iter.second->join();
    }
  }
}

Context::~Context() {
  join();
}

} // namespace tensorpipe
