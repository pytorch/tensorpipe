/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/listener.h>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Listener::Impl : public std::enable_shared_from_this<Listener::Impl> {
 public:
  Impl(std::shared_ptr<Loop>, std::shared_ptr<TCPHandle>);

  // Called to initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Called to queue a callback to be called when a connection comes in.
  void acceptFromLoop(accept_callback_fn fn);

  // Called to obtain the listener's address.
  std::string addrFromLoop() const;

  // Called to shut down the connection and its resources.
  void closeFromLoop();

 private:
  // This function is called by the event loop if the listening socket can
  // accept a new connection. Status is 0 in case of success, < 0
  // otherwise. See `uv_connection_cb` for more information.
  void connectionCallbackFromLoop_(int status);

  // Called when libuv has closed the handle.
  void closeCallbackFromLoop_();

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<TCPHandle> handle_;
  // TODO Add proper error handling.

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

  // By having the instance store a shared_ptr to itself we create a reference
  // cycle which will "leak" the instance. This allows us to detach its
  // lifetime from the connection and sync it with the TCPHandle's life cycle.
  std::shared_ptr<Impl> leak_;
};

Listener::Impl::Impl(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(std::move(loop)), handle_(std::move(handle)) {}

void Listener::Impl::initFromLoop() {
  leak_ = shared_from_this();
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop_(); });
  handle_->listenFromLoop(
      [this](int status) { this->connectionCallbackFromLoop_(status); });
}

void Listener::Impl::acceptFromLoop(accept_callback_fn fn) {
  callback_.arm(std::move(fn));
}

std::string Listener::Impl::addrFromLoop() const {
  return handle_->sockNameFromLoop().str();
}

void Listener::Impl::closeFromLoop() {
  handle_->closeFromLoop();
}

void Listener::Impl::connectionCallbackFromLoop_(int status) {
  TP_DCHECK(loop_->inLoopThread());
  if (status != 0) {
    callback_.trigger(
        TP_CREATE_ERROR(UVError, status), std::shared_ptr<Connection>());
    return;
  }

  auto connection = TCPHandle::create(loop_);
  connection->initFromLoop();
  handle_->acceptFromLoop(connection);
  callback_.trigger(
      Error::kSuccess, Connection::create_(loop_, std::move(connection)));
}

void Listener::Impl::closeCallbackFromLoop_() {
  leak_.reset();
}

std::shared_ptr<Listener> Listener::create_(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  auto handle = TCPHandle::create(loop);
  loop->deferToLoop([handle, addr]() {
    handle->initFromLoop();
    handle->bindFromLoop(addr);
  });
  auto listener =
      std::make_shared<Listener>(ConstructorToken(), loop, std::move(handle));
  listener->init_();
  return listener;
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(std::move(loop)),
      impl_(std::make_shared<Impl>(loop_, std::move(handle))) {}

void Listener::init_() {
  loop_->deferToLoop([impl{impl_}]() { impl->initFromLoop(); });
}

void Listener::accept(accept_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, fn{std::move(fn)}]() mutable {
    impl->acceptFromLoop(std::move(fn));
  });
}

address_t Listener::addr() const {
  std::string addr;
  loop_->runInLoop([this, &addr]() { addr = this->impl_->addrFromLoop(); });
  return addr;
}

Listener::~Listener() {
  loop_->deferToLoop([impl{impl_}]() { impl->closeFromLoop(); });
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
