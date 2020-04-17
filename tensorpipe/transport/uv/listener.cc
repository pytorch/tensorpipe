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
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Listener::Impl : public std::enable_shared_from_this<Listener::Impl> {
 public:
  // Create a listener that listens on the specified address.
  Impl(std::shared_ptr<Context::PrivateIface>, address_t);

  // Initialize member fields that need `shared_from_this`.
  void init();

  // Queue a callback to be called when a connection comes in.
  void accept(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addr() const;

  // Shut down the connection and its resources.
  void close();

 private:
  // Initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Queue a callback to be called when a connection comes in.
  void acceptFromLoop(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addrFromLoop() const;

  // Shut down the connection and its resources.
  void closeFromLoop();

  // Called by libuv if the listening socket can accept a new connection. Status
  // is 0 in case of success, < 0 otherwise. See `uv_connection_cb` for more
  // information.
  void connectionCallbackFromLoop_(int status);

  // Called when libuv has closed the handle.
  void closeCallbackFromLoop_();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<TCPHandle> handle_;
  Sockaddr sockaddr_;
  // TODO Add proper error handling.
  ClosingReceiver closingReceiver_;

  // Once an accept callback fires, it becomes disarmed and must be rearmed.
  // Any firings that occur while the callback is disarmed are stashed and
  // triggered as soon as it's rearmed. With libuv we don't have the ability
  // to disable the lower-level callback when the user callback is disarmed.
  // So we'll keep getting notified of new connections even if we don't know
  // what to do with them and don't want them. Thus we must store them
  // somewhere. This is what RearmableCallback is for.
  LocklessRearmableCallback<
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
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr)
    : context_(std::move(context)),
      handle_(context_->createHandle()),
      sockaddr_(Sockaddr::createInetSockAddr(addr)),
      closingReceiver_(context_, context_->getClosingEmitter()) {}

void Listener::Impl::initFromLoop() {
  leak_ = shared_from_this();

  closingReceiver_.activate(*this);

  handle_->initFromLoop();
  handle_->bindFromLoop(sockaddr_);
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

void Listener::Impl::close() {
  context_->deferToLoop(
      [impl{shared_from_this()}]() { impl->closeFromLoop(); });
}

void Listener::Impl::closeFromLoop() {
  handle_->closeFromLoop();
}

void Listener::Impl::connectionCallbackFromLoop_(int status) {
  TP_DCHECK(context_->inLoopThread());
  if (status != 0) {
    callback_.trigger(
        TP_CREATE_ERROR(UVError, status), std::shared_ptr<Connection>());
    return;
  }

  auto connection = context_->createHandle();
  connection->initFromLoop();
  handle_->acceptFromLoop(connection);
  callback_.trigger(
      Error::kSuccess,
      std::make_shared<Connection>(
          Connection::ConstructorToken(), context_, std::move(connection)));
}

void Listener::Impl::closeCallbackFromLoop_() {
  leak_.reset();
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr)
    : impl_(std::make_shared<Impl>(std::move(context), std::move(addr))) {
  impl_->init();
}

void Listener::Impl::init() {
  context_->deferToLoop([impl{shared_from_this()}]() { impl->initFromLoop(); });
}

void Listener::accept(accept_callback_fn fn) {
  impl_->accept(std::move(fn));
}

void Listener::Impl::accept(accept_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, fn{std::move(fn)}]() mutable {
        impl->acceptFromLoop(std::move(fn));
      });
}

address_t Listener::addr() const {
  return impl_->addr();
}

address_t Listener::Impl::addr() const {
  std::string addr;
  context_->runInLoop([this, &addr]() { addr = addrFromLoop(); });
  return addr;
}

void Listener::close() {
  impl_->close();
}

Listener::~Listener() {
  close();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
