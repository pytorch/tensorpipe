/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/uv.h>

#include <array>
#include <sstream>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/uv/macros.h>

namespace tensorpipe {
namespace transport {
namespace uv {

void TCPHandle::initFromLoop() {
  TP_DCHECK(this->loop_.inLoopThread());
  leak();
  int rv;
  rv = uv_tcp_init(loop_.ptr(), this->ptr());
  TP_THROW_UV_IF(rv < 0, rv);
  rv = uv_tcp_nodelay(this->ptr(), 1);
  TP_THROW_UV_IF(rv < 0, rv);
}

int TCPHandle::bindFromLoop(const Sockaddr& addr) {
  TP_DCHECK(this->loop_.inLoopThread());
  auto rv = uv_tcp_bind(ptr(), addr.addr(), 0);
  // We don't throw in case of errors here because sometimes we bind in order to
  // try if an address works and want to handle errors gracefully.
  return rv;
}

Sockaddr TCPHandle::sockNameFromLoop() {
  TP_DCHECK(this->loop_.inLoopThread());
  struct sockaddr_storage ss;
  struct sockaddr* addr = reinterpret_cast<struct sockaddr*>(&ss);
  int addrlen = sizeof(ss);
  auto rv = uv_tcp_getsockname(ptr(), addr, &addrlen);
  TP_THROW_UV_IF(rv < 0, rv);
  return Sockaddr(addr, addrlen);
}

void TCPHandle::connectFromLoop(
    const Sockaddr& addr,
    ConnectRequest::TConnectCallback fn) {
  TP_DCHECK(this->loop_.inLoopThread());
  auto request = ConnectRequest::create(loop_, std::move(fn));
  auto rv =
      uv_tcp_connect(request->ptr(), ptr(), addr.addr(), request->callback());
  TP_THROW_UV_IF(rv < 0, rv);
}

std::tuple<int, Addrinfo> getAddrinfoFromLoop(
    Loop& loop,
    std::string hostname) {
  struct addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  uv_getaddrinfo_t request;
  // Don't use a callback, and thus perform the call synchronously, because the
  // asynchronous version uses a thread pool, and it's not worth spawning new
  // threads for a functionality which is used so sparingly.
  auto rv = uv_getaddrinfo(
      loop.ptr(),
      &request,
      /*getaddrinfo_cb=*/nullptr,
      hostname.c_str(),
      /*service=*/nullptr,
      &hints);
  if (rv != 0) {
    return std::make_tuple(rv, Addrinfo());
  }

  return std::make_tuple(0, Addrinfo(request.addrinfo, AddrinfoDeleter()));
}

std::tuple<int, InterfaceAddresses, int> getInterfaceAddresses() {
  uv_interface_address_t* info;
  int count;
  auto rv = uv_interface_addresses(&info, &count);
  if (rv != 0) {
    return std::make_tuple(rv, InterfaceAddresses(), 0);
  }
  return std::make_tuple(
      0, InterfaceAddresses(info, InterfaceAddressesDeleter(count)), count);
}

std::tuple<int, std::string> getHostname() {
  std::array<char, UV_MAXHOSTNAMESIZE> hostname;
  size_t size = hostname.size();
  auto rv = uv_os_gethostname(hostname.data(), &size);
  if (rv != 0) {
    return std::make_tuple(rv, std::string());
  }
  return std::make_tuple(
      0, std::string(hostname.data(), hostname.data() + size));
}

std::string formatUvError(int status) {
  if (status == 0) {
    return "success";
  } else {
    std::ostringstream ss;
    ss << uv_err_name(status) << ": " << uv_strerror(status);
    return ss.str();
  }
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
