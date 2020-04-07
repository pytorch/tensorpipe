/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <list>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/proto/channel/basic.pb.h>

namespace tensorpipe {
namespace channel {
namespace basic {

class Context : public channel::Context {
 public:
  Context();

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  class PrivateIface {
   public:
    virtual ClosingEmitter& getClosingEmitter() = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl : public PrivateIface, public std::enable_shared_from_this<Impl> {
   public:
    Impl();

    const std::string& domainDescriptor() const;

    std::shared_ptr<Channel> createChannel(
        std::shared_ptr<transport::Connection>,
        Channel::Endpoint);

    ClosingEmitter& getClosingEmitter() override;

    void close();

    void join();

    ~Impl() override = default;

   private:
    std::string domainDescriptor_;
    std::atomic<bool> closed_{false};
    std::atomic<bool> joined_{false};
    ClosingEmitter closingEmitter_;
  };

  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  std::shared_ptr<Impl> impl_;

  friend class Channel;
};

} // namespace basic
} // namespace channel
} // namespace tensorpipe
