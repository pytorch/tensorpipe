/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/config.h>
#include <tensorpipe/core/buffer_helpers.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/transport/context.h>

#include <tensorpipe/channel/cpu_context.h>
#if TENSORPIPE_SUPPORTS_CUDA
#include <tensorpipe/channel/cuda_context.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

namespace tensorpipe {

class ContextImpl final : public std::enable_shared_from_this<ContextImpl> {
 public:
  explicit ContextImpl(ContextOptions opts);

  void registerTransport(
      int64_t priority,
      std::string transport,
      std::shared_ptr<transport::Context> context);

  void registerChannel(
      int64_t priority,
      std::string channel,
      std::shared_ptr<channel::CpuContext> context);

#if TENSORPIPE_SUPPORTS_CUDA
  void registerChannel(
      int64_t priority,
      std::string channel,
      std::shared_ptr<channel::CudaContext> context);
#endif

  std::shared_ptr<Listener> listen(const std::vector<std::string>& urls);

  std::shared_ptr<Pipe> connect(const std::string& url, PipeOptions opts);

  ClosingEmitter& getClosingEmitter();

  std::shared_ptr<transport::Context> getTransport(
      const std::string& transport);
  std::shared_ptr<channel::CpuContext> getCpuChannel(
      const std::string& channel);
#if TENSORPIPE_SUPPORTS_CUDA
  std::shared_ptr<channel::CudaContext> getCudaChannel(
      const std::string& channel);
#endif // TENSORPIPE_SUPPORTS_CUDA

  using TOrderedTransports = std::map<
      int64_t,
      std::tuple<std::string, std::shared_ptr<transport::Context>>>;

  const TOrderedTransports& getOrderedTransports();

  template <typename TBuffer>
  using TOrderedChannels = std::map<
      int64_t,
      std::tuple<std::string, std::shared_ptr<channel::Context<TBuffer>>>>;

  const TOrderedChannels<CpuBuffer>& getOrderedCpuChannels();
#if TENSORPIPE_SUPPORTS_CUDA
  const TOrderedChannels<CudaBuffer>& getOrderedCudaChannels();
#endif // TENSORPIPE_SUPPORTS_CUDA

  // Return the name given to the context's constructor. It will be retrieved
  // by the pipes and listener in order to attach it to logged messages.
  const std::string& getName();

  void close();

  void join();

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
  void registerChannel(
      int64_t priority,
      std::string channel,
      std::shared_ptr<channel::Context<TBuffer>> context);

  template <typename TBuffer>
  std::shared_ptr<channel::Context<TBuffer>> getChannel(
      const std::string& channel);
};

} // namespace tensorpipe
