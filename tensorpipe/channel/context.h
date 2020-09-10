/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>
#include <vector>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {

// Abstract base class for channel context classes.
//
// Instances of these classes are expected to be registered with a
// context. All registered instances are assumed to be eligible
// channels for all pairs.
//
template <typename TTensor>
class Context {
 public:
  // Return string to describe the domain for this channel.
  //
  // Two processes with a channel context of the same type whose
  // domain descriptors are identical can connect to each other.
  //
  virtual const std::string& domainDescriptor() const = 0;

  // Return newly created channel using the specified connection.
  //
  // It is up to the channel to either use this connection for further
  // initialization, or use it directly. Either way, the returned
  // channel should be immediately usable. If the channel isn't fully
  // initialized yet, take care to queue these operations to execute
  // as soon as initialization has completed.
  //
  virtual std::shared_ptr<Channel<TTensor>> createChannel(
      std::shared_ptr<transport::Connection>,
      Endpoint) = 0;

  // Tell the context what its identifier is.
  //
  // This is only supposed to be called from the high-level context. It will
  // only used for logging and debugging purposes.
  virtual void setId(std::string id) = 0;

  // Put the channel context in a terminal state, in turn closing all of its
  // channels, and release its resources. This may be done asynchronously, in
  // background.
  virtual void close() = 0;

  // Wait for all resources to be released and all background activity to stop.
  virtual void join() = 0;

  virtual ~Context() = default;

 private:
  std::string name_;
};

using CpuContext = Context<CpuBuffer>;

#if TENSORPIPE_HAS_CUDA
using CudaContext = Context<CudaBuffer>;
#endif // TENSORPIPE_HAS_CUDA

} // namespace channel
} // namespace tensorpipe
