/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <functional>
#include <mutex>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/ibv/sockaddr.h>
#include <tensorpipe/transport/listener_impl_boilerplate.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

class ConnectionImpl;
class ContextImpl;

class ListenerImpl final
    : public ListenerImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>,
      public EpollLoop::EventHandler {
 public:
  // Create a listener that listens on the specified address.
  ListenerImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::string addr);

  // Implementation of EventHandler.
  void handleEventsFromLoop(int events) override;

 protected:
  // Implement the entry points called by ListenerImplBoilerplate.
  void initImplFromLoop() override;
  void acceptImplFromLoop(accept_callback_fn fn) override;
  std::string addrImplFromLoop() const override;
  void handleErrorImpl() override;

 private:
  Socket socket_;
  Sockaddr sockaddr_;
  std::deque<accept_callback_fn> fns_;
};

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
