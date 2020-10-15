/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/error.h>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <sstream>

#include <tensorpipe/common/ibv.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

std::string IbvError::what() const {
  return ibv_wc_status_str(status_);
}

std::string GetaddrinfoError::what() const {
  std::ostringstream ss;
  ss << "getaddrinfo: " << gai_strerror(error_);
  return ss.str();
}

std::string NoAddrFoundError::what() const {
  return "no address found";
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
