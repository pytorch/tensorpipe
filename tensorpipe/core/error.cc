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

std::string LogicError::what() const {
  std::ostringstream ss;
  ss << "logic error: " << reason_;
  return ss.str();
}

std::string PipeClosedError::what() const {
  std::ostringstream ss;
  ss << "pipe got closed";
  return ss.str();
}

} // namespace tensorpipe
