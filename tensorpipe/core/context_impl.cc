/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/context_impl.h>

#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/listener_impl.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/core/pipe_impl.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {

namespace {

std::atomic<uint64_t> contextCouter{0};

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

ContextImpl::ContextImpl(ContextOptions opts)
    : id_(createContextId()), name_(std::move(opts.name_)) {
  TP_VLOG(1) << "Context " << id_ << " created";
  if (name_ != "") {
    TP_VLOG(1) << "Context " << id_ << " aliased as " << name_;
    id_ = name_;
  }
}

void ContextImpl::init() {
  deferToLoop([this]() { initFromLoop(); });
}

void ContextImpl::initFromLoop() {}

void ContextImpl::registerTransport(
    int64_t priority,
    std::string transport,
    std::shared_ptr<transport::Context> context) {
  TP_THROW_ASSERT_IF(transport.empty());
  TP_THROW_ASSERT_IF(transports_.find(transport) != transports_.end())
      << "transport " << transport << " already registered";
  TP_THROW_ASSERT_IF(
      transportsByPriority_.find(-priority) != transportsByPriority_.end())
      << "transport with priority " << priority << " already registered";
  if (!context->isViable()) {
    TP_VLOG(1) << "Context " << id_ << " is not registering transport "
               << transport << " because it is not viable";
    return;
  }
  TP_VLOG(1) << "Context " << id_ << " is registering transport " << transport;
  context->setId(id_ + ".tr_" + transport);
  transports_.emplace(transport, context);
  // Reverse the priority, as the pipe will pick the *first* available transport
  // it can find in the ordered map, so higher priorities should come first.
  transportsByPriority_.emplace(-priority, std::make_tuple(transport, context));
}

void ContextImpl::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::Context> context) {
  TP_THROW_ASSERT_IF(channel.empty());
  TP_THROW_ASSERT_IF(channels_.find(channel) != channels_.end())
      << "channel " << channel << " already registered";
  TP_THROW_ASSERT_IF(
      channelsByPriority_.find(-priority) != channelsByPriority_.end())
      << "channel with priority " << priority << " already registered";
  if (!context->isViable()) {
    TP_VLOG(1) << "Context " << id_ << " is not registering channel " << channel
               << " because it is not viable";
    return;
  }
  TP_VLOG(1) << "Context " << id_ << " is registering channel " << channel;
  context->setId(id_ + ".ch_" + channel);
  channels_.emplace(channel, context);
  // Reverse the priority, as the pipe will pick the *first* available channel
  // it can find in the ordered map, so higher priorities should come first.
  channelsByPriority_.emplace(-priority, std::make_tuple(channel, context));
}

std::shared_ptr<Listener> ContextImpl::listen(
    const std::vector<std::string>& urls) {
  std::string listenerId =
      id_ + "[l" + std::to_string(listenerCounter_++) + "]";
  TP_VLOG(1) << "Context " << id_ << " is opening listener " << listenerId;
  return std::make_shared<Listener>(
      Listener::ConstructorToken(),
      shared_from_this(),
      std::move(listenerId),
      urls);
}

std::shared_ptr<Pipe> ContextImpl::connect(
    const std::string& url,
    PipeOptions opts) {
  std::string pipeId = id_ + ".p" + std::to_string(pipeCounter_++);
  TP_VLOG(1) << "Context " << id_ << " is opening pipe " << pipeId;
  std::string remoteContextName = std::move(opts.remoteName_);
  if (remoteContextName != "") {
    std::string aliasPipeId = id_ + "_to_" + remoteContextName;
    TP_VLOG(1) << "Pipe " << pipeId << " aliased as " << aliasPipeId;
    pipeId = std::move(aliasPipeId);
  }
  return std::make_shared<Pipe>(
      Pipe::ConstructorToken(),
      shared_from_this(),
      std::move(pipeId),
      std::move(remoteContextName),
      url);
}

std::shared_ptr<transport::Context> ContextImpl::getTransport(
    const std::string& transport) {
  auto iter = transports_.find(transport);
  if (iter == transports_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  return iter->second;
}

std::shared_ptr<channel::Context> ContextImpl::getChannel(
    const std::string& channel) {
  auto iter = channels_.find(channel);
  if (iter == channels_.end()) {
    TP_THROW_EINVAL() << "unsupported channel " << channel;
  }
  return iter->second;
}

const ContextImpl::TOrderedTransports& ContextImpl::getOrderedTransports() {
  return transportsByPriority_;
}

const ContextImpl::TOrderedChannels& ContextImpl::getOrderedChannels() {
  return channelsByPriority_;
}

const std::string& ContextImpl::getName() {
  return name_;
}

void ContextImpl::enroll(ListenerImpl& listener) {
  TP_DCHECK(inLoop());
  bool wasInserted;
  std::tie(std::ignore, wasInserted) =
      listeners_.emplace(&listener, listener.shared_from_this());
  TP_DCHECK(wasInserted);
}

void ContextImpl::enroll(PipeImpl& pipe) {
  TP_DCHECK(inLoop());
  bool wasInserted;
  std::tie(std::ignore, wasInserted) =
      pipes_.emplace(&pipe, pipe.shared_from_this());
  TP_DCHECK(wasInserted);
}

void ContextImpl::unenroll(ListenerImpl& listener) {
  TP_DCHECK(inLoop());
  auto numRemoved = listeners_.erase(&listener);
  TP_DCHECK_EQ(numRemoved, 1);
}

void ContextImpl::unenroll(PipeImpl& pipe) {
  TP_DCHECK(inLoop());
  auto numRemoved = pipes_.erase(&pipe);
  TP_DCHECK_EQ(numRemoved, 1);
}

bool ContextImpl::closed() {
  TP_DCHECK(inLoop());
  return error_;
}

void ContextImpl::deferToLoop(TTask fn) {
  loop_.deferToLoop(std::move(fn));
}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
}

void ContextImpl::close() {
  deferToLoop([this]() { closeFromLoop(); });
}

void ContextImpl::closeFromLoop() {
  TP_DCHECK(inLoop());
  TP_VLOG(1) << "Context " << id_ << " is closing";
  setError(TP_CREATE_ERROR(ContextClosedError));
  TP_VLOG(1) << "Context " << id_ << " done closing";
}

void ContextImpl::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void ContextImpl::handleError() {
  TP_DCHECK(inLoop());
  TP_VLOG(5) << "Context " << id_ << " is handling error " << error_.what();

  // Make a copy as they could unenroll themselves inline.
  auto listenersCopy = listeners_;
  auto pipesCopy = pipes_;
  // We call closeFromLoop, rather than just close, because we need these
  // objects to transition _immediately_ to error, "atomically". If we just
  // deferred closing to later, this could come after some already-enqueued
  // operations that could try to access the context, which would be closed,
  // and this could fail.
  for (auto& iter : listenersCopy) {
    iter.second->closeFromLoop();
  }
  for (auto& iter : pipesCopy) {
    iter.second->closeFromLoop();
  }

  for (auto& iter : transports_) {
    iter.second->close();
  }
  for (auto& iter : channels_) {
    iter.second->close();
  }
}

void ContextImpl::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(1) << "Context " << id_ << " is joining";

    // As closing is deferred to the loop, we must wait for close to be actually
    // called before we join, to avoid race conditions. For this, we defer
    // another task to the loop, which we know will run after the closing, and
    // then we wait for that task to be run.
    std::promise<void> hasClosed;
    deferToLoop([&]() { hasClosed.set_value(); });
    hasClosed.get_future().wait();

    for (auto& iter : transports_) {
      iter.second->join();
    }
    for (auto& iter : channels_) {
      iter.second->join();
    }

    TP_VLOG(1) << "Context " << id_ << " done joining";

    TP_DCHECK(listeners_.empty());
    TP_DCHECK(pipes_.empty());
  }
}

} // namespace tensorpipe
