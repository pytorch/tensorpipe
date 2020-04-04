/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/uv.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/uv/macros.h>

namespace tensorpipe {
namespace transport {
namespace uv {

void TCPHandle::initFromLoop() {
  TP_DCHECK(this->loop_->inLoopThread());
  leak();
  uv_tcp_init(loop_->ptr(), this->ptr());
}

void TCPHandle::bindFromLoop(const Sockaddr& addr) {
  TP_DCHECK(this->loop_->inLoopThread());
  auto rv = uv_tcp_bind(ptr(), addr.addr(), 0);
  TP_THROW_UV_IF(rv < 0, rv);
}

Sockaddr TCPHandle::sockNameFromLoop() {
  TP_DCHECK(this->loop_->inLoopThread());
  struct sockaddr_storage addr;
  int addrlen = sizeof(addr);
  auto rv = uv_tcp_getsockname(
      ptr(), reinterpret_cast<struct sockaddr*>(&addr), &addrlen);
  TP_THROW_UV_IF(rv < 0, rv);
  return Sockaddr(addr, addrlen);
}

void TCPHandle::connectFromLoop(
    const Sockaddr& addr,
    ConnectRequest::TConnectCallback fn) {
  TP_DCHECK(this->loop_->inLoopThread());
  auto request = ConnectRequest::create(loop_, std::move(fn));
  auto rv =
      uv_tcp_connect(request->ptr(), ptr(), addr.addr(), request->callback());
  TP_THROW_UV_IF(rv < 0, rv);
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
