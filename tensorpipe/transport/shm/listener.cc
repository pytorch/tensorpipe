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
  Impl(std::shared_ptr<Loop>, const Sockaddr& addr);

  // Called to initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Called to queue a callback to be called when a connection comes in.
  void acceptFromLoop(accept_callback_fn fn);

  // Called to obtain the listener's address.
  std::string addrFromLoop() const;

  void closeFromLoop();

  void handleEventsFromLoop(int events) override;

 private:
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> socket_;
  Sockaddr addr_;
  std::deque<accept_callback_fn> fns_;
};

std::shared_ptr<Listener> Listener::create_(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  return std::make_shared<Listener>(ConstructorToken(), std::move(loop), addr);
}

Listener::Impl::Impl(std::shared_ptr<Loop> loop, const Sockaddr& addr)
    : loop_(std::move(loop)),
      socket_(Socket::createForFamily(AF_UNIX)),
      addr_(addr) {}

void Listener::Impl::initFromLoop() {
  TP_DCHECK(loop_->inLoopThread());
  // Bind socket to abstract socket address.
  socket_->bind(addr_);
  socket_->block(false);
  socket_->listen(128);
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr)
    : loop_(loop), impl_(std::make_shared<Impl>(loop, addr)) {
  loop_->deferToLoop([impl{impl_}]() { impl->initFromLoop(); });
}

void Listener::Impl::closeFromLoop() {
  TP_DCHECK(loop_->inLoopThread());
  if (socket_) {
    if (!fns_.empty()) {
      loop_->unregisterDescriptor(socket_->fd());
    }
    socket_.reset();
  }
}

void Listener::close() {
  loop_->deferToLoop([impl{impl_}]() { impl->closeFromLoop(); });
}

Listener::~Listener() {
  close();
}

void Listener::accept(accept_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, fn{std::move(fn)}]() mutable {
    impl->acceptFromLoop(std::move(fn));
  });
}

void Listener::Impl::acceptFromLoop(accept_callback_fn fn) {
  TP_DCHECK(loop_->inLoopThread());
  fns_.push_back(std::move(fn));

  // Only register if we go from 0 to 1 pending callbacks. In other cases we
  // already had a pending callback and thus we were already registered.
  if (fns_.size() == 1) {
    // Register with loop for readability events.
    loop_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
  }
}

address_t Listener::addr() const {
  std::string addr;
  loop_->runInLoop([this, &addr]() { addr = this->impl_->addrFromLoop(); });
  return addr;
}

address_t Listener::Impl::addrFromLoop() const {
  TP_DCHECK(loop_->inLoopThread());
  return addr_.str();
}

void Listener::Impl::handleEventsFromLoop(int events) {
  TP_DCHECK(loop_->inLoopThread());
  TP_ARG_CHECK_EQ(events, EPOLLIN);
  TP_DCHECK(!fns_.empty())
      << "when the callback is disarmed the listener's descriptor is supposed "
      << "to be unregistered";

  auto fn = std::move(fns_.front());
  fns_.pop_front();
  if (fns_.empty()) {
    loop_->unregisterDescriptor(socket_->fd());
  }
  auto socket = socket_->accept();
  if (socket) {
    fn(Error::kSuccess, Connection::create_(loop_, socket));
  } else {
    fn(TP_CREATE_ERROR(SystemError, "accept", errno),
       std::shared_ptr<Connection>());
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
