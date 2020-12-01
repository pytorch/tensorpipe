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

#include <tensorpipe/channel/cpu_context.h>

namespace tensorpipe {
namespace channel {
namespace xth {

class Context : public channel::CpuContext {
 public:
  Context();

  const std::string& domainDescriptor() const override;

  std::shared_ptr<CpuChannel> createChannel(
      std::shared_ptr<transport::Connection> connection,
      Endpoint endpoint) override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  class PrivateIface;

  class Impl;

  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  std::shared_ptr<Impl> impl_;

  // Allow channel to see the private interface.
  friend class Channel;
};

} // namespace xth
} // namespace channel
} // namespace tensorpipe
