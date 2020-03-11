/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <unordered_set>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/listener.h>

#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Context;
class TCPHandle;

class Listener : public transport::Listener,
                 public std::enable_shared_from_this<Listener> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  Listener(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  ~Listener() override;

  using transport::Listener::accept_callback_fn;

  void accept(accept_callback_fn fn) override;

  address_t addr() const override;

 private:
  static std::shared_ptr<Listener> create(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  void start();

  class Impl : public std::enable_shared_from_this<Impl> {
   public:
    Impl(std::shared_ptr<Loop>, std::shared_ptr<TCPHandle>);

    void startFromLoop();

    void closeFromLoop();

    void closeCallbackFromLoop();

    void acceptFromLoop(accept_callback_fn fn);

    std::string addrFromLoop() const;

    std::shared_ptr<Loop> loop_;
    std::shared_ptr<TCPHandle> handle_;
    // Once an accept callback fires, it becomes disarmed and must be rearmed.
    // Any firings that occur while the callback is disarmed are stashed and
    // triggered as soon as it's rearmed. With libuv we don't have the ability
    // to disable the lower-level callback when the user callback is disarmed.
    // So we'll keep getting notified of new connections even if we don't know
    // what to do with them and don't want them. Thus we must store them
    // somewhere. This is what RearmableCallback is for.
    RearmableCallbackWithOwnLock<
        accept_callback_fn,
        const Error&,
        std::shared_ptr<Connection>>
        callback_;

    // This function is called by the event loop if the listening socket can
    // accept a new connection. Status is 0 in case of success, < 0
    // otherwise. See `uv_connection_cb` for more information.
    void connectionCallbackFromLoop(int status);

    // By having the instance store a shared_ptr to itself we create a reference
    // cycle which will "leak" the instance. This allows us to detach its
    // lifetime from the connection and sync it with the TCPHandle's life cycle.
    std::shared_ptr<Impl> leak_;
  };

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Impl> impl_;

  friend class Context;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
