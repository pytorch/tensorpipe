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

void TCPHandle::init() {
  loop_->deferToLoop(
      runIfAlive(*this, std::function<void(TCPHandle&)>([](TCPHandle& handle) {
        uv_tcp_init(handle.loop_->ptr(), &handle.handle_);
      })));
}

void TCPHandle::noDelay(bool enable) {
  loop_->deferToLoop(runIfAlive(
      *this, std::function<void(TCPHandle&)>([enable](TCPHandle& handle) {
        auto rv = uv_tcp_nodelay(&handle.handle_, enable ? 1 : 0);
        TP_THROW_UV_IF(rv < 0, rv);
      })));
}

void TCPHandle::bind(const Sockaddr& addr) {
  loop_->deferToLoop(runIfAlive(
      *this, std::function<void(TCPHandle&)>([addr](TCPHandle& handle) {
        auto rv = uv_tcp_bind(&handle.handle_, addr.addr(), 0);
        TP_THROW_UV_IF(rv < 0, rv);
      })));
}

Sockaddr TCPHandle::sockName() {
  struct sockaddr_storage addr;
  int addrlen = sizeof(addr);
  loop_->runInLoop([&] {
    auto rv = uv_tcp_getsockname(
        &handle_, reinterpret_cast<struct sockaddr*>(&addr), &addrlen);
    TP_THROW_UV_IF(rv < 0, rv);
  });
  return Sockaddr(addr, addrlen);
}

Sockaddr TCPHandle::peerName() {
  struct sockaddr_storage addr;
  int addrlen = sizeof(addr);
  loop_->runInLoop([&] {
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
  loop_->deferToLoop(runIfAlive(
      *this,
      std::function<void(TCPHandle&)>([addr, request{std::move(request)}](
                                          TCPHandle& handle) {
        auto rv = uv_tcp_connect(
            request->ptr(), &handle.handle_, addr.addr(), request->callback());
        TP_THROW_UV_IF(rv < 0, rv);
      })));
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
