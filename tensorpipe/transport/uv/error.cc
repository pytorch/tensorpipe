/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/error.h>

#include <sstream>

#include <uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::string UVError::what() const {
  std::ostringstream ss;
  ss << uv_err_name(error_) << ": " << uv_strerror(error_);
  return ss.str();
}

std::string NoAddrFoundError::what() const {
  return "no address found";
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
