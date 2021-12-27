/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/listener_impl_boilerplate.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class ConnectionImpl;
class ContextImpl;

class ListenerImpl final : public ListenerImplBoilerplate<
                               ContextImpl,
                               ListenerImpl,
                               ConnectionImpl> {
 public:
  // Create a listener that listens on the specified address.
  ListenerImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::string addr);

 protected:
  // Implement the entry points called by ListenerImplBoilerplate.
  void initImplFromLoop() override;
  void acceptImplFromLoop(accept_callback_fn fn) override;
  std::string addrImplFromLoop() const override;
  void handleErrorImpl() override;

 private:
  // Called by libuv if the listening socket can accept a new connection. Status
  // is 0 in case of success, < 0 otherwise. See `uv_connection_cb` for more
  // information.
  void connectionCallbackFromLoop(int status);

  // Called when libuv has closed the handle.
  void closeCallbackFromLoop();

  const std::unique_ptr<TCPHandle> handle_;
  Sockaddr sockaddr_;

  // Once an accept callback fires, it becomes disarmed and must be rearmed.
  // Any firings that occur while the callback is disarmed are stashed and
  // triggered as soon as it's rearmed. With libuv we don't have the ability
  // to disable the lower-level callback when the user callback is disarmed.
  // So we'll keep getting notified of new connections even if we don't know
  // what to do with them and don't want them. Thus we must store them
  // somewhere. This is what RearmableCallback is for.
  RearmableCallback<const Error&, std::shared_ptr<Connection>> callback_;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
