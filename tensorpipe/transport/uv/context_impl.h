/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <tuple>

#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/context_impl_boilerplate.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class ConnectionImpl;
class ListenerImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl> {
 public:
  ContextImpl();

  std::tuple<Error, std::string> lookupAddrForIface(std::string iface);

  std::tuple<Error, std::string> lookupAddrForHostname();

  // Implement the DeferredExecutor interface.
  bool inLoop() override;
  void deferToLoop(std::function<void()> fn) override;

  bool closed() {
    return loop_.closed();
  }

  std::unique_ptr<TCPHandle> createHandle();

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void closeImpl() override;
  void joinImpl() override;

 private:
  Loop loop_;

  std::tuple<Error, std::string> lookupAddrForHostnameFromLoop();
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
