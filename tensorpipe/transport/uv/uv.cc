/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/uv.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/error_macros.h>
#include <tensorpipe/transport/uv/macros.h>

namespace tensorpipe {
namespace transport {
namespace uv {

void TCPHandle::init() {
  loop_->run([&] { uv_tcp_init(loop_->ptr(), &handle_); });
}

void TCPHandle::noDelay(bool enable) {
  loop_->run([&] {
    auto rv = uv_tcp_nodelay(&handle_, enable ? 1 : 0);
    TP_THROW_UV_IF(rv < 0, rv);
  });
}

void TCPHandle::bind(const Sockaddr& addr) {
  loop_->run([&] {
    auto rv = uv_tcp_bind(&handle_, addr.addr(), 0);
    TP_THROW_UV_IF(rv < 0, rv);
  });
}

Sockaddr TCPHandle::sockName() {
  struct sockaddr_storage addr;
  int addrlen = sizeof(addr);
  loop_->run([&] {
    auto rv = uv_tcp_getsockname(
        &handle_, reinterpret_cast<struct sockaddr*>(&addr), &addrlen);
    TP_THROW_UV_IF(rv < 0, rv);
  });
  return Sockaddr(addr, addrlen);
}

Sockaddr TCPHandle::peerName() {
  struct sockaddr_storage addr;
  int addrlen = sizeof(addr);
  loop_->run([&] {
    auto rv = uv_tcp_getpeername(
        &handle_, reinterpret_cast<struct sockaddr*>(&addr), &addrlen);
    TP_THROW_UV_IF(rv < 0, rv);
  });
  return Sockaddr(addr, addrlen);
}

void TCPHandle::connect(const Sockaddr& addr) {
  connect(addr, [](int status) {});
}

void TCPHandle::connect(
    const Sockaddr& addr,
    ConnectRequest::TConnectCallback fn) {
  auto request = loop_->createRequest<ConnectRequest>(std::move(fn));
  loop_->run([&] {
    auto rv = uv_tcp_connect(
        request->ptr(), &handle_, addr.addr(), request->callback());
    TP_THROW_UV_IF(rv < 0, rv);
  });
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
