/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/listener.h>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/uv/connection.h>
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
  listener_ = loop_->createHandle<TCPHandle>();
  listener_->bind(addr);
}

Listener::~Listener() {
  if (listener_) {
    listener_->close();
  }
}

void Listener::start() {
  listener_->listen(runIfAlive(
      *this,
      std::function<void(Listener&, int)>([](Listener& listener, int status) {
        listener.connectionCallback(status);
      })));
}

Sockaddr Listener::sockaddr() {
  return listener_->sockName();
}

void Listener::accept(accept_callback_fn fn) {
  fn_.emplace(std::move(fn));
}

address_t Listener::addr() const {
  return listener_->sockName().str();
}

void Listener::connectionCallback(int status) {
  if (status != 0) {
    TP_LOG_WARNING() << "connection callback called with status " << status
                     << ": " << uv_strerror(status);
    return;
  }

  auto connection = loop_->createHandle<TCPHandle>();
  listener_->accept(connection);
  if (!fn_) {
    TP_LOG_WARNING() << "closing accepted connection because listener "
                     << "doesn't have an accept callback";
    connection->close();
    return;
  }

  fn_.value()(Connection::create(loop_, std::move(connection)));
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
