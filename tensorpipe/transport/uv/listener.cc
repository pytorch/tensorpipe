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
  loop_->deferToLoop([impl{impl_}]() { impl->closeFromLoop(); });
}

void Listener::Impl::closeFromLoop() {
  handle_->closeFromLoop();
}

void Listener::Impl::closeCallbackFromLoop() {
  leak_.reset();
}

void Listener::start() {
  loop_->deferToLoop([impl{impl_}]() { impl->startFromLoop(); });
}

void Listener::Impl::startFromLoop() {
  leak_ = shared_from_this();
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop(); });
  handle_->listenFromLoop(
      [this](int status) { this->connectionCallbackFromLoop(status); });
}

void Listener::accept(accept_callback_fn fn) {
  loop_->deferToLoop([impl{impl_}, fn{std::move(fn)}]() mutable {
    impl->acceptFromLoop(std::move(fn));
  });
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
  handle_->acceptFromLoop(connection);
  callback_.trigger(
      Error::kSuccess, Connection::create(loop_, std::move(connection)));
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
