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

#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/transport/context_impl_boilerplate.h>
#include <tensorpipe/transport/efa/reactor.h>

namespace tensorpipe {
namespace transport {
namespace efa {

class ConnectionImpl;
class ListenerImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl> {
 public:
  static std::shared_ptr<ContextImpl> create();

  ContextImpl(EfaLib efaLib, EfaDeviceList deviceList);
  ContextImpl();

  // Implement the DeferredExecutor interface.
  bool inLoop() const override;
  void deferToLoop(std::function<void()> fn) override;

  void registerDescriptor(
      int fd,
      int events,
      std::shared_ptr<EpollLoop::EventHandler> h);

  void unregisterDescriptor(int fd);

  Reactor& getReactor();

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void handleErrorImpl() override;
  void joinImpl() override;

 private:
  Reactor reactor_;
  EpollLoop loop_{this->reactor_};
};

} // namespace efa
} // namespace transport
} // namespace tensorpipe
