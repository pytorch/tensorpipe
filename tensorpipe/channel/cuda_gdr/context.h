/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <tensorpipe/channel/cuda_context.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

class ContextImpl;

class Context : public CudaContext {
 public:
  explicit Context(
      optional<std::vector<std::string>> gpuIdxToNicName = nullopt);

  Context(const Context&) = delete;
  Context(Context&&) = delete;
  Context& operator=(const Context&) = delete;
  Context& operator=(Context&&) = delete;

  std::shared_ptr<CudaChannel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint) override;

  size_t numConnectionsNeeded() const override;

  const std::string& domainDescriptor() const override;

  bool isViable() const override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it. However, its lifetime is tied to the one
  // of this public object since when the latter is destroyed the implementation
  // is closed and joined.
  const std::shared_ptr<ContextImpl> impl_;
};

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
