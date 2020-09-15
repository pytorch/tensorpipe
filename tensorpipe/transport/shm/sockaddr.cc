/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

Sockaddr::Sockaddr(const struct sockaddr* addr, socklen_t addrlen) {
  TP_ARG_CHECK(addr != nullptr);
  TP_ARG_CHECK_LE(addrlen, sizeof(addr_));
  std::memset(&addr_, 0, sizeof(addr_));
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

} // namespace shm
} // namespace transport
} // namespace tensorpipe
