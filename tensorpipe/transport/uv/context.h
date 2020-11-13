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
#include <tuple>

#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Connection;
class Listener;

class Context : public transport::Context {
 public:
  Context();

  std::shared_ptr<transport::Connection> connect(std::string addr) override;

  std::shared_ptr<transport::Listener> listen(std::string addr) override;

  const std::string& domainDescriptor() const override;

  std::tuple<Error, std::string> lookupAddrForIface(std::string iface);

  std::tuple<Error, std::string> lookupAddrForHostname();

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

  // Allow listener to see the private interface.
  friend class Listener;
  friend class ListenerImpl;
  // Allow connection to see the private interface.
  friend class Connection;
  friend class ConnectionImpl;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
