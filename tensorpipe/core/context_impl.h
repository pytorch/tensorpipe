/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <tuple>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/config.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/transport/context.h>

#include <tensorpipe/channel/cpu_context.h>
#if TENSORPIPE_SUPPORTS_CUDA
#include <tensorpipe/channel/cuda_context.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

namespace tensorpipe {

class Listener;
class Pipe;

class Context::PrivateIface {
 public:
  virtual ClosingEmitter& getClosingEmitter() = 0;

  virtual std::shared_ptr<transport::Context> getTransport(
      const std::string&) = 0;

  virtual std::shared_ptr<channel::CpuContext> getCpuChannel(
      const std::string&) = 0;

#if TENSORPIPE_SUPPORTS_CUDA
  virtual std::shared_ptr<channel::CudaContext> getCudaChannel(
      const std::string&) = 0;
#endif // TENSORPIPE_SUPPORTS_CUDA

  using TOrderedTransports = std::map<
      int64_t,
      std::tuple<std::string, std::shared_ptr<transport::Context>>>;

  virtual const TOrderedTransports& getOrderedTransports() = 0;

  template <typename TBuffer>
  using TOrderedChannels = std::map<
      int64_t,
      std::tuple<std::string, std::shared_ptr<channel::Context<TBuffer>>>>;

  virtual const TOrderedChannels<CpuBuffer>& getOrderedCpuChannels() = 0;

#if TENSORPIPE_SUPPORTS_CUDA
  virtual const TOrderedChannels<CudaBuffer>& getOrderedCudaChannels() = 0;
#endif // TENSORPIPE_SUPPORTS_CUDA

  // Return the name given to the context's constructor. It will be retrieved
  // by the pipes and listener in order to attach it to logged messages.
  virtual const std::string& getName() = 0;

  virtual ~PrivateIface() = default;
};

} // namespace tensorpipe
