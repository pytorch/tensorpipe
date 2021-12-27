/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {

std::string ContextClosedError::what() const {
  return "context closed";
}

std::string ListenerClosedError::what() const {
  return "listener closed";
}

std::string ConnectionClosedError::what() const {
  return "connection closed";
}

std::string ContextNotViableError::what() const {
  return "context not viable";
}

} // namespace transport
} // namespace tensorpipe
