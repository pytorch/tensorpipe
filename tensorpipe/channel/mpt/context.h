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

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

class Context : public channel::Context {
 public:
  Context(
      std::vector<std::shared_ptr<transport::Context>>,
      std::vector<std::shared_ptr<transport::Listener>>);

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint) override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  class PrivateIface {
   public:
    virtual ClosingEmitter& getClosingEmitter() = 0;

    using connection_request_callback_fn = std::function<
        void(const Error&, std::shared_ptr<transport::Connection>)>;

    virtual const std::vector<std::string>& addresses() const = 0;

    virtual uint64_t registerConnectionRequest(
        uint64_t laneIdx,
        connection_request_callback_fn) = 0;

    virtual void unregisterConnectionRequest(uint64_t) = 0;

    virtual std::shared_ptr<transport::Connection> connect(
        uint64_t laneIdx,
        std::string address) = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl;

  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  std::shared_ptr<Impl> impl_;

  // Allow channel to see the private interface.
  friend class Channel;
};

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
