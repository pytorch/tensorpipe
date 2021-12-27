/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/socket.h>

#include <cstring>
#include <string>

#include <tensorpipe/common/socket.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Sockaddr final : public tensorpipe::Sockaddr {
 public:
  static Sockaddr createInetSockAddr(const std::string& str);

  Sockaddr(const struct sockaddr* addr, socklen_t addrlen) {
    TP_ARG_CHECK(addr != nullptr);
    TP_ARG_CHECK_LE(addrlen, sizeof(addr_));
    // Ensure the sockaddr_storage is zeroed, because we don't always
    // write to all fields in the `sockaddr_[in|in6]` structures.
    std::memset(&addr_, 0, sizeof(addr_));
    std::memcpy(&addr_, addr, addrlen);
    addrlen_ = addrlen;
  }

  inline const struct sockaddr* addr() const override {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }

  inline struct sockaddr* addr() {
    return reinterpret_cast<struct sockaddr*>(&addr_);
  }

  inline socklen_t addrlen() const override {
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
