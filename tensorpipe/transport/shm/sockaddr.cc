/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/sockaddr.h>

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
  // There are three "modes" for binding UNIX domain sockets:
  // - if len(path) == 0: it autobinds to an abstract address
  // - if len(path) > 0 and path[0] == 0: it uses an explicit abstract address
  // - if len(path) > 0 and path[0] != 0: it uses a concrete filesystem path
  if (name == "") {
    return Sockaddr(
        reinterpret_cast<struct sockaddr*>(&sun), sizeof(sun.sun_family));
  } else {
    constexpr size_t offset = 1;
    const size_t len = std::min(sizeof(sun.sun_path) - offset, name.size());
    std::strncpy(&sun.sun_path[offset], name.data(), len);

    // Note: instead of using sizeof(sun) we compute the addrlen from
    // the string length of the abstract socket name. If we use
    // sizeof(sun), lsof shows all the trailing NUL characters.
    return Sockaddr(
        reinterpret_cast<struct sockaddr*>(&sun),
        sizeof(sun.sun_family) + offset + len);
  }
};

Sockaddr::Sockaddr(const struct sockaddr* addr, socklen_t addrlen) {
  TP_ARG_CHECK(addr != nullptr);
  TP_ARG_CHECK_LE(addrlen, sizeof(addr_));
  std::memset(&addr_, 0, sizeof(addr_));
  std::memcpy(&addr_, addr, addrlen);
  addrlen_ = addrlen;
}

std::string Sockaddr::str() const {
  TP_DCHECK_GE(addrlen_, sizeof(sockaddr_un::sun_family));
  if (addrlen_ == sizeof(sockaddr_un::sun_family)) {
    return "";
  } else {
    const struct sockaddr_un* sun{
        reinterpret_cast<const struct sockaddr_un*>(&addr_)};
    TP_DCHECK_EQ(sun->sun_path[0], '\0');
    constexpr size_t offset = 1;
    const size_t len = addrlen_ - sizeof(sun->sun_family) - offset;
    return std::string(&sun->sun_path[offset], len);
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
