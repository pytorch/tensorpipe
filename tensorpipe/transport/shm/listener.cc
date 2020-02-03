/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/listener.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

std::shared_ptr<Listener> Listener::create(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  return std::make_shared<Listener>(ConstructorToken(), std::move(loop), addr);
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr)
    : loop_(std::move(loop)),
      listener_(Socket::createForFamily(AF_UNIX)),
      addr_(addr) {
  // Bind socket to abstract socket address.
  listener_->bind(addr);
  listener_->block(false);
  listener_->listen(128);
}

Listener::~Listener() {
  if (fn_.has_value()) {
    loop_->unregisterDescriptor(listener_->fd());
  }
}

void Listener::accept(accept_callback_fn fn) {
  fn_.emplace(std::move(fn));

  // Register with loop for readability events.
  loop_->registerDescriptor(listener_->fd(), EPOLLIN, shared_from_this());
}

address_t Listener::addr() const {
  return addr_.str();
}

void Listener::handleEvents(int events) {
  TP_ARG_CHECK_EQ(events, EPOLLIN);

  if (!fn_.has_value()) {
    return;
  }

  auto fn = std::move(fn_).value();
  loop_->unregisterDescriptor(listener_->fd());
  fn_.reset();
  auto socket = listener_->accept();
  if (socket) {
    fn(Error::kSuccess, Connection::create(loop_, socket));
  } else {
    fn(TP_CREATE_ERROR(SystemError, "accept", errno),
       std::shared_ptr<Connection>());
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
