/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/listener_impl_boilerplate.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/context_impl.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class ListenerImpl final
    : public ListenerImplBoilerplate<ListenerImpl, Context::PrivateIface> {
 public:
  // Create a listener that listens on the specified address.
  ListenerImpl(
      std::shared_ptr<Context::PrivateIface>,
      std::string,
      std::string);

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

  std::shared_ptr<TCPHandle> handle_;
  Sockaddr sockaddr_;

  // Once an accept callback fires, it becomes disarmed and must be rearmed.
  // Any firings that occur while the callback is disarmed are stashed and
  // triggered as soon as it's rearmed. With libuv we don't have the ability
  // to disable the lower-level callback when the user callback is disarmed.
  // So we'll keep getting notified of new connections even if we don't know
  // what to do with them and don't want them. Thus we must store them
  // somewhere. This is what RearmableCallback is for.
  RearmableCallback<const Error&, std::shared_ptr<Connection>> callback_;

  // Sequence numbers for the connections created by this listener, used to
  // create their identifiers based off this listener's identifier. They will
  // only be used for logging and debugging.
  std::atomic<uint64_t> connectionCounter_{0};

  // By having the instance store a shared_ptr to itself we create a reference
  // cycle which will "leak" the instance. This allows us to detach its
  // lifetime from the connection and sync it with the TCPHandle's life cycle.
  std::shared_ptr<ListenerImpl> leak_;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
