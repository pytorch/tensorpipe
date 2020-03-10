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

std::shared_ptr<Listener> Listener::create(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  auto handle = TCPHandle::create(loop);
  loop->deferToLoop([handle, addr]() {
    handle->initFromLoop();
    handle->bindFromLoop(addr);
  });
  auto listener =
      std::make_shared<Listener>(ConstructorToken(), loop, std::move(handle));
  listener->start();
  return listener;
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(std::move(loop)),
      impl_(std::make_shared<Impl>(loop_, std::move(handle))) {}

Listener::Impl::Impl(
    std::shared_ptr<Loop> loop,
    std::shared_ptr<TCPHandle> handle)
    : loop_(std::move(loop)), handle_(std::move(handle)) {}

Listener::~Listener() {
  impl_->close();
}

void Listener::Impl::close() {
  for (const auto& connection : connectionsWaitingForAccept_) {
    connection->close();
  }
  handle_->close();
}

void Listener::start() {
  loop_->deferToLoop(
      runIfAlive(*this, std::function<void(Listener&)>([](Listener& listener) {
        listener.impl_->startFromLoop();
      })));
}

void Listener::Impl::startFromLoop() {
  handle_->listenFromLoop(runIfAlive(
      *this, std::function<void(Impl&, int)>([](Impl& impl, int status) {
        impl.connectionCallbackFromLoop(status);
      })));
}

void Listener::accept(accept_callback_fn fn) {
  loop_->deferToLoop(runIfAlive(
      *this,
      std::function<void(Listener&)>(
          [fn{std::move(fn)}](Listener& listener) mutable {
            listener.impl_->acceptFromLoop(std::move(fn));
          })));
}

void Listener::Impl::acceptFromLoop(accept_callback_fn fn) {
  callback_.arm(std::move(fn));
}

address_t Listener::addr() const {
  std::string addr;
  loop_->runInLoop([this, &addr]() { addr = this->impl_->addrFromLoop(); });
  return addr;
}

address_t Listener::Impl::addrFromLoop() const {
  return handle_->sockNameFromLoop().str();
}

void Listener::Impl::connectionCallbackFromLoop(int status) {
  TP_DCHECK(loop_->inLoopThread());
  if (status != 0) {
    callback_.trigger(
        TP_CREATE_ERROR(UVError, status), std::shared_ptr<Connection>());
    return;
  }

  auto connection = TCPHandle::create(loop_);
  connection->initFromLoop();
  connectionsWaitingForAccept_.insert(connection);
  // Since a reference to the new TCPHandle is stored in a member field of the
  // listener, the TCPHandle will still be alive inside the following callback
  // (because runIfAlice ensures that the listener is alive). However, if we
  // captured a shared_ptr, then the TCPHandle would be kept alive by the
  // callback even if the listener got destroyed. To avoid that we capture a
  // weak_ptr, which we're however sure we'll be able to lock.
  handle_->acceptFromLoop(
      connection,
      runIfAlive(
          *this,
          std::function<void(Impl&, int)>(
              [weakConnection{std::weak_ptr<TCPHandle>(connection)}](
                  Impl& impl, int status) {
                std::shared_ptr<TCPHandle> sameConnection =
                    weakConnection.lock();
                TP_DCHECK(sameConnection);
                impl.acceptCallbackFromLoop(std::move(sameConnection), status);
              })));
}

void Listener::Impl::acceptCallbackFromLoop(
    std::shared_ptr<TCPHandle> connection,
    int status) {
  TP_DCHECK(loop_->inLoopThread());
  connectionsWaitingForAccept_.erase(connection);
  if (status != 0) {
    connection->close();
    callback_.trigger(
        TP_CREATE_ERROR(UVError, status), std::shared_ptr<Connection>());
    return;
  }
  callback_.trigger(
      Error::kSuccess, Connection::create(loop_, std::move(connection)));
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
