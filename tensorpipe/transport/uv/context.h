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

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Connection;
class Listener;
class TCPHandle;

class Context : public transport::Context {
 public:
  Context();

  std::shared_ptr<transport::Connection> connect(address_t addr) override;

  std::shared_ptr<transport::Listener> listen(address_t addr) override;

  const std::string& domainDescriptor() const override;

  std::tuple<Error, address_t> lookupAddrForIface(std::string iface);

  std::tuple<Error, address_t> lookupAddrForHostname();

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  class PrivateIface {
   public:
    virtual ClosingEmitter& getClosingEmitter() = 0;

    virtual bool inLoopThread() = 0;

    virtual void deferToLoop(std::function<void()> fn) = 0;

    virtual void runInLoop(std::function<void()> fn) = 0;

    virtual std::shared_ptr<TCPHandle> createHandle() = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl;

  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  std::shared_ptr<Impl> impl_;

  // Allow listener to see the private interface.
  friend class Listener;
  // Allow connection to see the private interface.
  friend class Connection;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
