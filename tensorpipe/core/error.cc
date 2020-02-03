/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/error.h>

#include <sstream>

namespace tensorpipe {

const Error Error::kSuccess = Error();

std::string TransportError::what() const {
  std::ostringstream ss;
  ss << "transport error: " << error_.what();
  return ss.str();
}

std::string LogicError::what() const {
  std::ostringstream ss;
  ss << "logic error: " << reason_;
  return ss.str();
}

} // namespace tensorpipe
