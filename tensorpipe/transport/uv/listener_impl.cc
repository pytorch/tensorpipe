/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/listener_impl.h>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/uv/connection_impl.h>
#include <tensorpipe/transport/uv/context_impl.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

ListenerImpl::ListenerImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string addr)
    : ListenerImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      handle_(context_->createHandle()),
      sockaddr_(Sockaddr::createInetSockAddr(addr)) {}

void ListenerImpl::initImplFromLoop() {
  context_->enroll(*this);

  TP_VLOG(9) << "Listener " << id_ << " is initializing in loop";

  TP_THROW_ASSERT_IF(context_->closed());
  handle_->initFromLoop();
  auto rv = handle_->bindFromLoop(sockaddr_);
  TP_THROW_UV_IF(rv < 0, rv);
  handle_->armCloseCallbackFromLoop(
      [this]() { this->closeCallbackFromLoop(); });
  handle_->listenFromLoop(
      [this](int status) { this->connectionCallbackFromLoop(status); });

  sockaddr_ = handle_->sockNameFromLoop();
}

void ListenerImpl::acceptImplFromLoop(accept_callback_fn fn) {
  callback_.arm(std::move(fn));
}

std::string ListenerImpl::addrImplFromLoop() const {
  return sockaddr_.str();
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
  TP_THROW_ASSERT_IF(context_->closed());
  connection->initFromLoop();
  handle_->acceptFromLoop(*connection);
  callback_.trigger(
      Error::kSuccess, createAndInitConnection(std::move(connection)));
}

void ListenerImpl::closeCallbackFromLoop() {
  TP_VLOG(9) << "Listener " << id_ << " has finished closing its handle";
  context_->unenroll(*this);
}

void ListenerImpl::handleErrorImpl() {
  callback_.triggerAll([&]() {
    return std::make_tuple(std::cref(error_), std::shared_ptr<Connection>());
  });
  handle_->closeFromLoop();
  // Do NOT unenroll here, as we must keep the UV handle alive until the close
  // callback fires.
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
