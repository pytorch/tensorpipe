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
  Impl(std::shared_ptr<Context::PrivateIface>, address_t, std::string);

  // Initialize member fields that need `shared_from_this`.
  void init();

  // Queue a callback to be called when a connection comes in.
  void accept(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addr() const;

  // Tell the listener what its identifier is.
  void setId(std::string id);

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

  void setError_(Error error);

  // Deal with an error.
  void handleError_();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<TCPHandle> handle_;
  Sockaddr sockaddr_;
  Error error_{Error::kSuccess};
  ClosingReceiver closingReceiver_;

  // Once an accept callback fires, it becomes disarmed and must be rearmed.
  // Any firings that occur while the callback is disarmed are stashed and
  // triggered as soon as it's rearmed. With libuv we don't have the ability
  // to disable the lower-level callback when the user callback is disarmed.
  // So we'll keep getting notified of new connections even if we don't know
  // what to do with them and don't want them. Thus we must store them
  // somewhere. This is what RearmableCallback is for.
  RearmableCallback<const Error&, std::shared_ptr<Connection>> callback_;

  // An identifier for the listener, composed of the identifier for the context,
  // combined with an increasing sequence number. It will be used as a prefix
  // for the identifiers of connections. All of them will only be used for
  // logging and debugging purposes.
  std::string id_;

  // Sequence numbers for the connections created by this listener, used to
  // create their identifiers based off this listener's identifier. They will
  // only be used for logging and debugging.
  uint64_t connectionCounter_{0};

  // By having the instance store a shared_ptr to itself we create a reference
  // cycle which will "leak" the instance. This allows us to detach its
  // lifetime from the connection and sync it with the TCPHandle's life cycle.
  std::shared_ptr<Impl> leak_;
};

Listener::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr,
    std::string id)
    : context_(std::move(context)),
      handle_(context_->createHandle()),
      sockaddr_(Sockaddr::createInetSockAddr(addr)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

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
  TP_DCHECK(context_->inLoopThread());

  if (error_) {
    fn(error_, std::shared_ptr<Connection>());
    return;
  }

  callback_.arm(std::move(fn));
}

std::string Listener::Impl::addrFromLoop() const {
  TP_DCHECK(context_->inLoopThread());
  return handle_->sockNameFromLoop().str();
}

void Listener::Impl::setId(std::string id) {
  TP_VLOG() << "Listener " << id_ << " was renamed to " << id;
  // FIXME Should we defer this to the loop?
  id_ = std::move(id);
}

void Listener::Impl::close() {
  context_->deferToLoop(
      [impl{shared_from_this()}]() { impl->closeFromLoop(); });
}

void Listener::Impl::closeFromLoop() {
  setError_(TP_CREATE_ERROR(ListenerClosedError));
}

void Listener::Impl::connectionCallbackFromLoop_(int status) {
  TP_DCHECK(context_->inLoopThread());

  if (status != 0) {
    setError_(TP_CREATE_ERROR(UVError, status));
    return;
  }

  auto connection = context_->createHandle();
  connection->initFromLoop();
  handle_->acceptFromLoop(connection);
  std::string connectionId = id_ + ".c" + std::to_string(connectionCounter_++);
  TP_VLOG() << "Listener " << id_ << " is opening connection " << connectionId;
  callback_.trigger(
      Error::kSuccess,
      std::make_shared<Connection>(
          Connection::ConstructorToken(),
          context_,
          std::move(connection),
          std::move(connectionId)));
}

void Listener::Impl::closeCallbackFromLoop_() {
  leak_.reset();
}

void Listener::Impl::setError_(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError_();
}

void Listener::Impl::handleError_() {
  callback_.triggerAll([&]() {
    return std::make_tuple(std::cref(error_), std::shared_ptr<Connection>());
  });
  handle_->closeFromLoop();
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(addr),
          std::move(id))) {
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

void Listener::setId(std::string id) {
  impl_->setId(std::move(id));
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
