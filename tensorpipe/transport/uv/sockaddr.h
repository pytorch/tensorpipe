/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/socket.h>

#include <cstring>
#include <string>

namespace tensorpipe {
namespace transport {
namespace uv {

class Sockaddr final {
 public:
  static Sockaddr createInetSockAddr(const std::string& name);

  Sockaddr(const struct sockaddr* addr, socklen_t addrlen) : addrlen_(addrlen) {
    // Ensure the sockaddr_storage is zeroed, because we don't always
    // write to all fields in the `sockaddr_[in|in6]` structures.
    std::memset(&addr_, 0, sizeof(addr_));
    std::memcpy(&addr_, addr, addrlen);
  }

  inline const struct sockaddr* addr() const {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }

  inline struct sockaddr* addr() {
    return reinterpret_cast<struct sockaddr*>(&addr_);
  }

  inline socklen_t addrlen() const {
    return addrlen_;
  }

  std::string str() const;

 private:
  struct sockaddr_storage addr_;
  socklen_t addrlen_;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
