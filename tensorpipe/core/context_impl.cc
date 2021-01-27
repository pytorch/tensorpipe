/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/context_impl.h>

#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/core/buffer_helpers.h>
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

ContextImpl::ContextImpl(ContextOptions opts)
    : id_(createContextId()), name_(std::move(opts.name_)) {
  TP_VLOG(1) << "Context " << id_ << " created";
  if (name_ != "") {
    TP_VLOG(1) << "Context " << id_ << " aliased as " << name_;
    id_ = name_;
  }
}

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

template <typename TBuffer>
void ContextImpl::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::Context<TBuffer>> context) {
  auto& channels = channels_.get<TBuffer>();
  auto& channelsByPriority = channelsByPriority_.get<TBuffer>();
  TP_THROW_ASSERT_IF(channel.empty());
  TP_THROW_ASSERT_IF(channels.find(channel) != channels.end())
      << "channel " << channel << " already registered";
  TP_THROW_ASSERT_IF(
      channelsByPriority.find(-priority) != channelsByPriority.end())
      << "channel with priority " << priority << " already registered";
  if (!context->isViable()) {
    TP_VLOG(1) << "Context " << id_ << " is not registering channel " << channel
               << " because it is not viable";
    return;
  }
  TP_VLOG(1) << "Context " << id_ << " is registering channel " << channel;
  context->setId(id_ + ".ch_" + channel);
  channels.emplace(channel, context);
  // Reverse the priority, as the pipe will pick the *first* available channel
  // it can find in the ordered map, so higher priorities should come first.
  channelsByPriority.emplace(-priority, std::make_tuple(channel, context));
}

void ContextImpl::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::CpuContext> context) {
  registerChannel<CpuBuffer>(priority, std::move(channel), std::move(context));
}

#if TENSORPIPE_SUPPORTS_CUDA
void ContextImpl::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::CudaContext> context) {
  registerChannel<CudaBuffer>(priority, std::move(channel), std::move(context));
}
#endif

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

ClosingEmitter& ContextImpl::getClosingEmitter() {
  return closingEmitter_;
}

std::shared_ptr<transport::Context> ContextImpl::getTransport(
    const std::string& transport) {
  auto iter = transports_.find(transport);
  if (iter == transports_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  return iter->second;
}

template <typename TBuffer>
std::shared_ptr<channel::Context<TBuffer>> ContextImpl::getChannel(
    const std::string& channel) {
  auto& channels = channels_.get<TBuffer>();
  auto iter = channels.find(channel);
  if (iter == channels.end()) {
    TP_THROW_EINVAL() << "unsupported channel " << channel;
  }
  return iter->second;
}

std::shared_ptr<channel::CpuContext> ContextImpl::getCpuChannel(
    const std::string& channel) {
  return getChannel<CpuBuffer>(channel);
}

#if TENSORPIPE_SUPPORTS_CUDA
std::shared_ptr<channel::CudaContext> ContextImpl::getCudaChannel(
    const std::string& channel) {
  return getChannel<CudaBuffer>(channel);
}
#endif // TENSORPIPE_SUPPORTS_CUDA

const ContextImpl::TOrderedTransports& ContextImpl::getOrderedTransports() {
  return transportsByPriority_;
}

const ContextImpl::TOrderedChannels<CpuBuffer>& ContextImpl::
    getOrderedCpuChannels() {
  return channelsByPriority_.get<CpuBuffer>();
}

#if TENSORPIPE_SUPPORTS_CUDA
const ContextImpl::TOrderedChannels<CudaBuffer>& ContextImpl::
    getOrderedCudaChannels() {
  return channelsByPriority_.get<CudaBuffer>();
}
#endif // TENSORPIPE_SUPPORTS_CUDA

const std::string& ContextImpl::getName() {
  return name_;
}

void ContextImpl::close() {
  if (!closed_.exchange(true)) {
    TP_VLOG(1) << "Context " << id_ << " is closing";

    closingEmitter_.close();

    for (auto& iter : transports_) {
      iter.second->close();
    }
    forEachDeviceType([&](auto buffer) {
      for (auto& iter : channels_.get<decltype(buffer)>()) {
        iter.second->close();
      }
    });

    TP_VLOG(1) << "Context " << id_ << " done closing";
  }
}

void ContextImpl::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(1) << "Context " << id_ << " is joining";

    for (auto& iter : transports_) {
      iter.second->join();
    }
    forEachDeviceType([&](auto buffer) {
      for (auto& iter : channels_.get<decltype(buffer)>()) {
        iter.second->join();
      }
    });

    TP_VLOG(1) << "Context " << id_ << " done joining";
  }
}

} // namespace tensorpipe
