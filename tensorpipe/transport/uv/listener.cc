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
  auto listener =
      std::make_shared<Listener>(ConstructorToken(), std::move(loop), addr);
  listener->start();
  return listener;
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr)
    : loop_(std::move(loop)) {
  handle_ = loop_->createHandle<TCPHandle>();
  handle_->bind(addr);
}

Listener::~Listener() {
  for (const auto& connection : connectionsWaitingForAccept_) {
    connection->close();
  }
  if (handle_) {
    handle_->close();
  }
}

void Listener::start() {
  handle_->listen(runIfAlive(
      *this,
      std::function<void(Listener&, int)>([](Listener& listener, int status) {
        listener.connectionCallbackFromLoop(status);
      })));
}

Sockaddr Listener::sockaddr() {
  return handle_->sockName();
}

void Listener::accept(accept_callback_fn fn) {
  callback_.arm(std::move(fn));
}

address_t Listener::addr() const {
  return handle_->sockName().str();
}

void Listener::connectionCallbackFromLoop(int status) {
  TP_DCHECK(loop_->inLoopThread());
  if (status != 0) {
    callback_.trigger(
        TP_CREATE_ERROR(UVError, status), std::shared_ptr<Connection>());
    return;
  }

  auto connection = loop_->createHandle<TCPHandle>();
  connectionsWaitingForAccept_.insert(connection);
  // Since a reference to the new TCPHandle is stored in a member field of the
  // listener, the TCPHandle will still be alive inside the following callback
  // (because runIfAlice ensures that the listener is alive). However, if we
  // captured a shared_ptr, then the TCPHandle would be kept alive by the
  // callback even if the listener got destroyed. To avoid that we capture a
  // weak_ptr, which we're however sure we'll be able to lock.
  handle_->accept(
      connection,
      runIfAlive(
          *this,
          std::function<void(Listener&, int)>(
              [weakConnection{std::weak_ptr<TCPHandle>(connection)}](
                  Listener& listener, int status) {
                std::shared_ptr<TCPHandle> sameConnection =
                    weakConnection.lock();
                TP_DCHECK(sameConnection);
                listener.acceptCallbackFromLoop(
                    std::move(sameConnection), status);
              })));
}

void Listener::acceptCallbackFromLoop(
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
