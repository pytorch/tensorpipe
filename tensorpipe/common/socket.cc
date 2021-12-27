/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/socket.h>

#include <fcntl.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>

#include <tensorpipe/common/defs.h>

#ifndef SOCK_NONBLOCK
#define SOCK_NONBLOCK 0
#endif // SOCK_NONBLOCK

namespace tensorpipe {

std::tuple<Error, Socket> Socket::createForFamily(sa_family_t aiFamily) {
  auto rv = socket(aiFamily, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (rv == -1) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "socket", errno), Socket());
  }
  Socket sock(rv);
#ifndef SOCK_NONBLOCK
  // The SOCK_NONBLOCK option of socket() is Linux-only. On OSX, we need to
  // manually set the socket to non-blocking after its creation.
  auto err = sock->block(false);
  if (err) {
    return std::make_tuple(err, Socket());
  }
#endif // SOCK_NONBLOCK
  return std::make_tuple(Error::kSuccess, std::move(sock));
}

Error Socket::block(bool on) {
  int rv;
  rv = fcntl(fd_, F_GETFL);
  if (rv == -1) {
    return TP_CREATE_ERROR(SystemError, "fcntl", errno);
  }
  if (!on) {
    // Set O_NONBLOCK
    rv |= O_NONBLOCK;
  } else {
    // Clear O_NONBLOCK
    rv &= ~O_NONBLOCK;
  }
  rv = fcntl(fd_, F_SETFL, rv);
  if (rv == -1) {
    return TP_CREATE_ERROR(SystemError, "fcntl", errno);
  }
  return Error::kSuccess;
}

Error Socket::reuseAddr(bool on) {
  int onInt = on ? 1 : 0;
  auto rv = setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &onInt, sizeof(onInt));
  if (rv == -1) {
    return TP_CREATE_ERROR(SystemError, "setsockopt", errno);
  }
  return Error::kSuccess;
}

Error Socket::bind(const Sockaddr& addr) {
  auto rv = ::bind(fd_, addr.addr(), addr.addrlen());
  if (rv == -1) {
    return TP_CREATE_ERROR(SystemError, "bind", errno);
  }
  return Error::kSuccess;
}

Error Socket::listen(int backlog) {
  auto rv = ::listen(fd_, backlog);
  if (rv == -1) {
    return TP_CREATE_ERROR(SystemError, "listen", errno);
  }
  return Error::kSuccess;
}

std::tuple<Error, Socket> Socket::accept() {
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);
  int rv = -1;
  for (;;) {
    rv = ::accept(fd_, (struct sockaddr*)&addr, &addrlen);
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      return std::make_tuple(
          TP_CREATE_ERROR(SystemError, "accept", errno), Socket());
    }
    break;
  }
  return std::make_tuple(Error::kSuccess, Socket(rv));
}

Error Socket::connect(const Sockaddr& addr) {
  for (;;) {
    auto rv = ::connect(fd_, addr.addr(), addr.addrlen());
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      if (errno != EINPROGRESS) {
        return TP_CREATE_ERROR(SystemError, "connect", errno);
      }
    }
    break;
  }
  return Error::kSuccess;
}

std::tuple<Error, struct sockaddr_storage, socklen_t> Socket::getSockName()
    const {
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);
  int rv = ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &addrlen);
  if (rv < 0) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "getsockname", errno), addr, addrlen);
  }
  return std::make_tuple(Error::kSuccess, addr, addrlen);
}

} // namespace tensorpipe
