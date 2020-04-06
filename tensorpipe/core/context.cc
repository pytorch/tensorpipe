/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/context.h>

#include <atomic>
#include <thread>
#include <unordered_map>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
  // Use the passkey idiom to allow make_shared to call what should be a
  // private constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Impl> create();

  Impl(ConstructorToken);

  void registerTransport(
      int64_t,
      std::string,
      std::shared_ptr<transport::Context>);

  void registerChannelFactory(
      int64_t,
      std::string,
      std::shared_ptr<channel::ChannelFactory>);

  std::shared_ptr<Listener> listen(const std::vector<std::string>&);

  std::shared_ptr<Pipe> connect(const std::string&);

  ClosingEmitter& getClosingEmitter() override;

  std::shared_ptr<transport::Context> getContextForTransport(
      const std::string&) override;
  std::shared_ptr<channel::ChannelFactory> getChannelFactory(
      const std::string&) override;

  using PrivateIface::TOrderedChannelFactories;

  const TOrderedContexts& getOrderedContexts() override;

  using PrivateIface::TOrderedContexts;

  const TOrderedChannelFactories& getOrderedChannelFactories() override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  std::unordered_map<std::string, std::shared_ptr<transport::Context>>
      contexts_;
  std::unordered_map<std::string, std::shared_ptr<channel::ChannelFactory>>
      channelFactories_;

  TOrderedContexts contextsByPriority_;
  TOrderedChannelFactories channelFactoriesByPriority_;

  ClosingEmitter closingEmitter_;
};

std::shared_ptr<Context> Context::create() {
  return std::make_shared<Context>(ConstructorToken());
}

Context::Context(ConstructorToken /* unused */) : impl_(Impl::create()) {}

std::shared_ptr<Context::Impl> Context::Impl::create() {
  return std::make_shared<Context::Impl>(ConstructorToken());
}

Context::Impl::Impl(ConstructorToken /* unused */) {}

void Context::registerTransport(
    int64_t priority,
    std::string transport,
    std::shared_ptr<transport::Context> context) {
  impl_->registerTransport(priority, std::move(transport), std::move(context));
}

void Context::Impl::registerTransport(
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
  impl_->registerChannelFactory(
      priority, std::move(name), std::move(channelFactory));
}

void Context::Impl::registerChannelFactory(
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

std::shared_ptr<Listener> Context::listen(
    const std::vector<std::string>& urls) {
  return impl_->listen(urls);
}

std::shared_ptr<Listener> Context::Impl::listen(
    const std::vector<std::string>& urls) {
  return std::make_shared<Listener>(
      Listener::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      urls);
}

std::shared_ptr<Pipe> Context::connect(const std::string& url) {
  return impl_->connect(url);
}

std::shared_ptr<Pipe> Context::Impl::connect(const std::string& url) {
  return std::make_shared<Pipe>(
      Pipe::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      url);
}

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
}

std::shared_ptr<transport::Context> Context::Impl::getContextForTransport(
    const std::string& transport) {
  auto iter = contexts_.find(transport);
  if (iter == contexts_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  return iter->second;
}

std::shared_ptr<channel::ChannelFactory> Context::Impl::getChannelFactory(
    const std::string& name) {
  auto iter = channelFactories_.find(name);
  if (iter == channelFactories_.end()) {
    TP_THROW_EINVAL() << "unsupported channel factory " << name;
  }
  return iter->second;
}

const Context::Impl::TOrderedContexts& Context::Impl::getOrderedContexts() {
  return contextsByPriority_;
}

const Context::Impl::TOrderedChannelFactories& Context::Impl::
    getOrderedChannelFactories() {
  return channelFactoriesByPriority_;
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
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
  impl_->join();
}

void Context::Impl::join() {
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
