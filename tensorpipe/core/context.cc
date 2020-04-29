/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/context.h>

#include <sys/types.h>
#include <unistd.h>

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

namespace {

uint64_t contextCouter{0};

std::string createContextId() {
  // Should we use argv[0] instead of the PID? It may be more semantically
  // meaningful and consistent across runs, but it may not be unique...
  // Also, should we add the hostname/the IP address in case the logs from
  // different hosts are merged into a single stream?
  // Eventually we'll have to replace getpid with something more portable.
  // Libuv offers a cross-platform function to get the process ID.
  return std::to_string(getpid()) + ":c" + std::to_string(contextCouter++);
}

} // namespace

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  explicit Impl(std::string name);

  void registerTransport(
      int64_t,
      std::string,
      std::shared_ptr<transport::Context>);

  void registerChannel(int64_t, std::string, std::shared_ptr<channel::Context>);

  std::shared_ptr<Listener> listen(const std::vector<std::string>&);

  std::shared_ptr<Pipe> connect(const std::string&, std::string name);

  ClosingEmitter& getClosingEmitter() override;

  std::shared_ptr<transport::Context> getTransport(const std::string&) override;
  std::shared_ptr<channel::Context> getChannel(const std::string&) override;

  using PrivateIface::TOrderedTransports;

  const TOrderedTransports& getOrderedTransports() override;

  using PrivateIface::TOrderedChannels;

  const TOrderedChannels& getOrderedChannels() override;

  const std::string& getName() override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  // An identifier for the context, composed of unique information about the
  // host and process, combined with an increasing sequence number. It will be
  // used as a prefix for the identifiers of listeners and pipes. All of them
  // will only be used for logging and debugging purposes.
  std::string id_;

  // Sequence numbers for the listeners and pipes created by this context, used
  // to create their identifiers based off this context's identifier. They will
  // only be used for logging and debugging.
  uint64_t listenerCounter_{0};
  uint64_t pipeCounter_{0};

  // A user-provided name for this context which should be semantically
  // meaningful. It will only be used for logging and debugging purposes, to
  // identify the endpoints of a pipe.
  std::string name_;

  std::unordered_map<std::string, std::shared_ptr<transport::Context>>
      transports_;
  std::unordered_map<std::string, std::shared_ptr<channel::Context>> channels_;

  TOrderedTransports transportsByPriority_;
  TOrderedChannels channelsByPriority_;

  ClosingEmitter closingEmitter_;
};

Context::Context(std::string name)
    : impl_(std::make_shared<Context::Impl>(std::move(name))) {}

Context::Impl::Impl(std::string name)
    : id_(createContextId()), name_(std::move(name)) {
  TP_VLOG() << "Context " << id_ << " corresponds to " << name_;
}

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
  std::string listenerId = id_ + ".l" + std::to_string(listenerCounter_++);
  TP_VLOG() << "Context " << id_ << " is opening listener " << listenerId;
  return std::make_shared<Listener>(
      Listener::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(listenerId),
      urls);
}

std::shared_ptr<Pipe> Context::connect(
    const std::string& url,
    std::string name) {
  return impl_->connect(url, std::move(name));
}

std::shared_ptr<Pipe> Context::Impl::connect(
    const std::string& url,
    std::string name) {
  std::string pipeId = id_ + ".p" + std::to_string(pipeCounter_++);
  TP_VLOG() << "Context " << id_ << " is opening pipe " << pipeId << " (from "
            << name_ << " to " << std::move(name) << ")";
  return std::make_shared<Pipe>(
      Pipe::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(pipeId),
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

const std::string& Context::Impl::getName() {
  return name_;
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  if (!closed_.exchange(true)) {

    TP_VLOG() << "Context " << id_ << " is closing";

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

  if (!joined_.exchange(true)) {
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
