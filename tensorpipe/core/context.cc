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
 public:
  Impl();

  void registerTransport(
      int64_t,
      std::string,
      std::shared_ptr<transport::Context>);

  void registerChannel(int64_t, std::string, std::shared_ptr<channel::Context>);

  std::shared_ptr<Listener> listen(const std::vector<std::string>&);

  std::shared_ptr<Pipe> connect(const std::string&);

  ClosingEmitter& getClosingEmitter() override;

  std::shared_ptr<transport::Context> getTransport(const std::string&) override;
  std::shared_ptr<channel::Context> getChannel(const std::string&) override;

  using PrivateIface::TOrderedTransports;

  const TOrderedTransports& getOrderedTransports() override;

  using PrivateIface::TOrderedChannels;

  const TOrderedChannels& getOrderedChannels() override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  std::unordered_map<std::string, std::shared_ptr<transport::Context>>
      transports_;
  std::unordered_map<std::string, std::shared_ptr<channel::Context>> channels_;

  TOrderedTransports transportsByPriority_;
  TOrderedChannels channelsByPriority_;

  ClosingEmitter closingEmitter_;
};

Context::Context() : impl_(std::make_shared<Context::Impl>()) {}

Context::Impl::Impl() {}

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
  TP_THROW_ASSERT_IF(transports_.find(transport) != transports_.end())
      << "transport " << transport << " already registered";
  TP_THROW_ASSERT_IF(
      transportsByPriority_.find(priority) != transportsByPriority_.end())
      << "transport with priority " << priority << " already registered";
  transports_.emplace(transport, context);
  transportsByPriority_.emplace(priority, std::make_tuple(transport, context));
}

void Context::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::Context> context) {
  impl_->registerChannel(priority, std::move(channel), std::move(context));
}

void Context::Impl::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::Context> context) {
  TP_THROW_ASSERT_IF(channel.empty());
  TP_THROW_ASSERT_IF(channels_.find(channel) != channels_.end())
      << "channel " << channel << " already registered";
  TP_THROW_ASSERT_IF(
      channelsByPriority_.find(priority) != channelsByPriority_.end())
      << "channel with priority " << priority << " already registered";
  channels_.emplace(channel, context);
  channelsByPriority_.emplace(priority, std::make_tuple(channel, context));
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

std::shared_ptr<transport::Context> Context::Impl::getTransport(
    const std::string& transport) {
  auto iter = transports_.find(transport);
  if (iter == transports_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  return iter->second;
}

std::shared_ptr<channel::Context> Context::Impl::getChannel(
    const std::string& channel) {
  auto iter = channels_.find(channel);
  if (iter == channels_.end()) {
    TP_THROW_EINVAL() << "unsupported channel " << channel;
  }
  return iter->second;
}

const Context::Impl::TOrderedTransports& Context::Impl::getOrderedTransports() {
  return transportsByPriority_;
}

const Context::Impl::TOrderedChannels& Context::Impl::getOrderedChannels() {
  return channelsByPriority_;
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  bool wasClosed = false;
  if (closed_.compare_exchange_strong(wasClosed, true)) {
    TP_DCHECK(!wasClosed);

    closingEmitter_.close();

    for (auto& iter : transports_) {
      iter.second->close();
    }
    for (auto& iter : channels_) {
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

    for (auto& iter : transports_) {
      iter.second->join();
    }
    for (auto& iter : channels_) {
      iter.second->join();
    }
  }
}

Context::~Context() {
  join();
}

} // namespace tensorpipe
