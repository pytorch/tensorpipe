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

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/socket.h>
#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Sockaddr final : public tensorpipe::Sockaddr {
 public:
  static Sockaddr createAbstractUnixAddr(const std::string& name);

  inline const struct sockaddr* addr() const override {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }

  inline socklen_t addrlen() const override {
    return addrlen_;
  }

  std::string str() const;

  explicit Sockaddr(const struct sockaddr* addr, socklen_t addrlen);

 private:
  struct sockaddr_storage addr_;
  socklen_t addrlen_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
