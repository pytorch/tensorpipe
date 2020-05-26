/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/socket.h>

#include <fcntl.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

Sockaddr Sockaddr::createAbstractUnixAddr(const std::string& name) {
  struct sockaddr_un sun;
  sun.sun_family = AF_UNIX;
  std::memset(&sun.sun_path, 0, sizeof(sun.sun_path));
  constexpr size_t offset = 1;
  const size_t len = std::min(sizeof(sun.sun_path) - offset, name.size());
  std::strncpy(&sun.sun_path[offset], name.c_str(), len);

  // Note: instead of using sizeof(sun) we compute the addrlen from
  // the string length of the abstract socket name. If we use
  // sizeof(sun), lsof shows all the trailing NUL characters.
  return Sockaddr(
      reinterpret_cast<struct sockaddr*>(&sun),
      sizeof(sun.sun_family) + offset + len + 1);
};

Sockaddr::Sockaddr(struct sockaddr* addr, socklen_t addrlen) {
  TP_ARG_CHECK_LE(addrlen, sizeof(addr_));
  std::memcpy(&addr_, addr, addrlen);
  addrlen_ = addrlen;
}

std::string Sockaddr::str() const {
  const struct sockaddr_un* sun{
      reinterpret_cast<const struct sockaddr_un*>(&addr_)};
  constexpr size_t offset = 1;
  const size_t len = addrlen_ - sizeof(sun->sun_family) - offset - 1;
  return std::string(&sun->sun_path[offset], len);
}

std::tuple<Error, std::shared_ptr<Socket>> Socket::createForFamily(
    sa_family_t ai_family) {
  auto rv = socket(ai_family, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (rv == -1) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "socket", errno),
        std::shared_ptr<Socket>());
  }
  return std::make_tuple(Error::kSuccess, std::make_shared<Socket>(rv));
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

std::tuple<Error, std::shared_ptr<Socket>> Socket::accept() {
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
          TP_CREATE_ERROR(SystemError, "accept", errno),
          std::shared_ptr<Socket>());
    }
    break;
  }
  return std::make_tuple(Error::kSuccess, std::make_shared<Socket>(rv));
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

} // namespace shm
} // namespace transport
} // namespace tensorpipe
