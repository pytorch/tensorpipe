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
#include <tensorpipe/core/buffer_helpers.h>
#include <tensorpipe/core/context_impl.h>
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
  explicit Impl(ContextOptions opts);

  void registerTransport(
      int64_t priority,
      std::string transport,
      std::shared_ptr<transport::Context> context);

  template <typename TBuffer>
  void registerChannel(
      int64_t priority,
      std::string channel,
      std::shared_ptr<channel::Context<TBuffer>> context);

  std::shared_ptr<Listener> listen(const std::vector<std::string>& urls);

  std::shared_ptr<Pipe> connect(const std::string& url, PipeOptions opts);

  ClosingEmitter& getClosingEmitter() override;

  std::shared_ptr<transport::Context> getTransport(
      const std::string& transport) override;
  std::shared_ptr<channel::CpuContext> getCpuChannel(
      const std::string& channel) override;
#if TENSORPIPE_SUPPORTS_CUDA
  std::shared_ptr<channel::CudaContext> getCudaChannel(
      const std::string& channel) override;
#endif // TENSORPIPE_SUPPORTS_CUDA

  using PrivateIface::TOrderedTransports;

  const TOrderedTransports& getOrderedTransports() override;

  using PrivateIface::TOrderedChannels;

  const TOrderedChannels<CpuBuffer>& getOrderedCpuChannels() override;
#if TENSORPIPE_SUPPORTS_CUDA
  const TOrderedChannels<CudaBuffer>& getOrderedCudaChannels() override;
#endif // TENSORPIPE_SUPPORTS_CUDA

  const std::string& getName() override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  // An identifier for the context, either consisting of the user-provided name
  // for this context (see below) or, by default, composed of unique information
  // about the host and process, combined with an increasing sequence number. It
  // will be used as a prefix for the identifiers of listeners and pipes. All of
  // them will only be used for logging and debugging purposes.
  std::string id_;

  // Sequence numbers for the listeners and pipes created by this context, used
  // to create their identifiers based off this context's identifier. They will
  // only be used for logging and debugging.
  std::atomic<uint64_t> listenerCounter_{0};
  std::atomic<uint64_t> pipeCounter_{0};

  // A user-provided name for this context which should be semantically
  // meaningful. It will only be used for logging and debugging purposes, to
  // identify the endpoints of a pipe.
  std::string name_;

  std::unordered_map<std::string, std::shared_ptr<transport::Context>>
      transports_;

  template <typename TBuffer>
  using TContextMap = std::
      unordered_map<std::string, std::shared_ptr<channel::Context<TBuffer>>>;
  TP_DEVICE_FIELD(TContextMap<CpuBuffer>, TContextMap<CudaBuffer>) channels_;

  TOrderedTransports transportsByPriority_;

  TP_DEVICE_FIELD(TOrderedChannels<CpuBuffer>, TOrderedChannels<CudaBuffer>)
  channelsByPriority_;

  ClosingEmitter closingEmitter_;

  template <typename TBuffer>
  std::shared_ptr<channel::Context<TBuffer>> getChannel(
      const std::string& channel);
};

Context::Context(ContextOptions opts)
    : impl_(std::make_shared<Context::Impl>(std::move(opts))) {}

Context::Impl::Impl(ContextOptions opts)
    : id_(createContextId()), name_(std::move(opts.name_)) {
  TP_VLOG(1) << "Context " << id_ << " created";
  if (name_ != "") {
    TP_VLOG(1) << "Context " << id_ << " aliased as " << name_;
    id_ = name_;
  }
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
void Context::Impl::registerChannel(
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

void Context::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::CpuContext> context) {
  impl_->registerChannel(priority, std::move(channel), std::move(context));
}

#if TENSORPIPE_SUPPORTS_CUDA
void Context::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::CudaContext> context) {
  impl_->registerChannel(priority, std::move(channel), std::move(context));
}
#endif // TENSORPIPE_SUPPORTS_CUDA

std::shared_ptr<Listener> Context::listen(
    const std::vector<std::string>& urls) {
  return impl_->listen(urls);
}

std::shared_ptr<Listener> Context::Impl::listen(
    const std::vector<std::string>& urls) {
  std::string listenerId =
      id_ + "[l" + std::to_string(listenerCounter_++) + "]";
  TP_VLOG(1) << "Context " << id_ << " is opening listener " << listenerId;
  return std::make_shared<Listener>(
      Listener::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(listenerId),
      urls);
}

std::shared_ptr<Pipe> Context::connect(
    const std::string& url,
    PipeOptions opts) {
  return impl_->connect(url, std::move(opts));
}

std::shared_ptr<Pipe> Context::Impl::connect(
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
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(pipeId),
      std::move(remoteContextName),
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

template <typename TBuffer>
std::shared_ptr<channel::Context<TBuffer>> Context::Impl::getChannel(
    const std::string& channel) {
  auto& channels = channels_.get<TBuffer>();
  auto iter = channels.find(channel);
  if (iter == channels.end()) {
    TP_THROW_EINVAL() << "unsupported channel " << channel;
  }
  return iter->second;
}

std::shared_ptr<channel::CpuContext> Context::Impl::getCpuChannel(
    const std::string& channel) {
  return getChannel<CpuBuffer>(channel);
}

#if TENSORPIPE_SUPPORTS_CUDA
std::shared_ptr<channel::CudaContext> Context::Impl::getCudaChannel(
    const std::string& channel) {
  return getChannel<CudaBuffer>(channel);
}
#endif // TENSORPIPE_SUPPORTS_CUDA

const Context::Impl::TOrderedTransports& Context::Impl::getOrderedTransports() {
  return transportsByPriority_;
}

const Context::Impl::TOrderedChannels<CpuBuffer>& Context::Impl::
    getOrderedCpuChannels() {
  return channelsByPriority_.get<CpuBuffer>();
}

#if TENSORPIPE_SUPPORTS_CUDA
const Context::Impl::TOrderedChannels<CudaBuffer>& Context::Impl::
    getOrderedCudaChannels() {
  return channelsByPriority_.get<CudaBuffer>();
}
#endif // TENSORPIPE_SUPPORTS_CUDA

const std::string& Context::Impl::getName() {
  return name_;
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
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

void Context::join() {
  impl_->join();
}

void Context::Impl::join() {
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

Context::~Context() {
  join();
}

} // namespace tensorpipe
