/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {

std::string EOFError::what() const {
  return "eof";
}

std::string ListenerClosedError::what() const {
  return "listener closed";
}

std::string ConnectionClosedError::what() const {
  return "connection closed";
}

} // namespace transport
} // namespace tensorpipe
