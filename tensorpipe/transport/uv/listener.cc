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
#include <tensorpipe/transport/uv/connection_impl.h>
#include <tensorpipe/transport/uv/context_impl.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/listener_impl.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<ContextImpl> context,
    std::string addr,
    std::string id)
    : ListenerBoilerplate<ListenerImpl, ContextImpl>(
          std::make_shared<ListenerImpl>(
              std::move(context),
              std::move(addr),
              std::move(id))) {}

ListenerImpl::ListenerImpl(
    std::shared_ptr<ContextImpl> context,
    std::string addr,
    std::string id)
    : ListenerImplBoilerplate<ListenerImpl, ContextImpl>(
          std::move(context),
          std::move(id)),
      handle_(context_->createHandle()),
      sockaddr_(Sockaddr::createInetSockAddr(addr)) {}

void ListenerImpl::initImplFromLoop() {
  leak_ = shared_from_this();

  handle_->initFromLoop();
  auto rv = handle_->bindFromLoop(sockaddr_);
  TP_THROW_UV_IF(rv < 0, rv);
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop(); });
  handle_->listenFromLoop(
      [this](int status) { this->connectionCallbackFromLoop(status); });
}

void ListenerImpl::acceptImplFromLoop(accept_callback_fn fn) {
  callback_.arm(std::move(fn));
}

std::string ListenerImpl::addrImplFromLoop() const {
  return handle_->sockNameFromLoop().str();
}

void ListenerImpl::connectionCallbackFromLoop(int status) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Listener " << id_
             << " has an incoming connection ready to be accepted ("
             << formatUvError(status) << ")";

  if (status != 0) {
    setError(TP_CREATE_ERROR(UVError, status));
    return;
  }

  auto connection = context_->createHandle();
  connection->initFromLoop();
  handle_->acceptFromLoop(connection);
  std::string connectionId = id_ + ".c" + std::to_string(connectionCounter_++);
  TP_VLOG(7) << "Listener " << id_ << " is opening connection " << connectionId;
  callback_.trigger(
      Error::kSuccess,
      std::make_shared<Connection>(
          Connection::ConstructorToken(),
          context_,
          std::move(connection),
          std::move(connectionId)));
}

void ListenerImpl::closeCallbackFromLoop() {
  TP_VLOG(9) << "Listener " << id_ << " has finished closing its handle";
  leak_.reset();
}

void ListenerImpl::handleErrorImpl() {
  callback_.triggerAll([&]() {
    return std::make_tuple(std::cref(error_), std::shared_ptr<Connection>());
  });
  handle_->closeFromLoop();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
