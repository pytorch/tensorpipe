/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/listener.h>

#include <deque>
#include <functional>
#include <mutex>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/socket.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Listener::Impl : public std::enable_shared_from_this<Listener::Impl>,
                       public EventHandler {
 public:
  // Create a listener that listens on the specified address.
  Impl(std::shared_ptr<Context::PrivateIface> context, address_t addr);

  // Initialize member fields that need `shared_from_this`.
  void init();

  // Queue a callback to be called when a connection comes in.
  void accept(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addr() const;

  // Shut down the connection and its resources.
  void close();

  // Implementation of EventHandler.
  void handleEventsFromLoop(int events) override;

 private:
  // Initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Queue a callback to be called when a connection comes in.
  void acceptFromLoop(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addrFromLoop() const;

  // Shut down the connection and its resources.
  void closeFromLoop();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<Socket> socket_;
  Sockaddr sockaddr_;
  std::deque<accept_callback_fn> fns_;
  ClosingReceiver closingReceiver_;
};

Listener::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr)
    : context_(std::move(context)),
      socket_(Socket::createForFamily(AF_UNIX)),
      sockaddr_(Sockaddr::createAbstractUnixAddr(addr)),
      closingReceiver_(context_, context_->getClosingEmitter()) {}

void Listener::Impl::initFromLoop() {
  TP_DCHECK(context_->inLoopThread());

  closingReceiver_.activate(*this);

  socket_->bind(sockaddr_);
  socket_->block(false);
  socket_->listen(128);
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

void Listener::Impl::closeFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  if (socket_) {
    if (!fns_.empty()) {
      context_->unregisterDescriptor(socket_->fd());
    }
    socket_.reset();
  }
}

void Listener::close() {
  impl_->close();
}

void Listener::Impl::close() {
  context_->deferToLoop(
      [impl{shared_from_this()}]() { impl->closeFromLoop(); });
}

Listener::~Listener() {
  close();
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

void Listener::Impl::acceptFromLoop(accept_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());
  fns_.push_back(std::move(fn));

  // Only register if we go from 0 to 1 pending callbacks. In other cases we
  // already had a pending callback and thus we were already registered.
  if (fns_.size() == 1) {
    // Register with loop for readability events.
    context_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
  }
}

address_t Listener::addr() const {
  return impl_->addr();
}

std::string Listener::Impl::addr() const {
  std::string addr;
  context_->runInLoop([this, &addr]() { addr = addrFromLoop(); });
  return addr;
}

address_t Listener::Impl::addrFromLoop() const {
  TP_DCHECK(context_->inLoopThread());
  return sockaddr_.str();
}

void Listener::Impl::handleEventsFromLoop(int events) {
  TP_DCHECK(context_->inLoopThread());
  TP_ARG_CHECK_EQ(events, EPOLLIN);
  TP_DCHECK(!fns_.empty())
      << "when the callback is disarmed the listener's descriptor is supposed "
      << "to be unregistered";

  auto fn = std::move(fns_.front());
  fns_.pop_front();
  if (fns_.empty()) {
    context_->unregisterDescriptor(socket_->fd());
  }
  auto socket = socket_->accept();
  if (socket) {
    fn(Error::kSuccess,
       std::make_shared<Connection>(
           Connection::ConstructorToken(), context_, std::move(socket)));
  } else {
    fn(TP_CREATE_ERROR(SystemError, "accept", errno),
       std::shared_ptr<Connection>());
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
