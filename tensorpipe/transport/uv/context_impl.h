/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
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

class ContextImpl final : public ContextImplBoilerplate<ContextImpl> {
 public:
  ContextImpl();

  std::shared_ptr<transport::Connection> connect(std::string addr);

  std::shared_ptr<transport::Listener> listen(std::string addr);

  std::tuple<Error, std::string> lookupAddrForIface(std::string iface);

  std::tuple<Error, std::string> lookupAddrForHostname();

  // Implement the DeferredExecutor interface.
  bool inLoop() override;
  void deferToLoop(std::function<void()> fn) override;

  std::shared_ptr<TCPHandle> createHandle();

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void closeImpl() override;
  void joinImpl() override;

 private:
  Loop loop_;

  // Sequence numbers for the listeners and connections created by this context,
  // used to create their identifiers based off this context's identifier. They
  // will only be used for logging and debugging.
  std::atomic<uint64_t> listenerCounter_{0};
  std::atomic<uint64_t> connectionCounter_{0};

  std::tuple<Error, std::string> lookupAddrForHostnameFromLoop();
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
