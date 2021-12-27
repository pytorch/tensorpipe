/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/error.h>

#include <cstring>
#include <sstream>

namespace tensorpipe {
namespace channel {

std::string ContextClosedError::what() const {
  return "context closed";
}

std::string ChannelClosedError::what() const {
  return "channel closed";
}

std::string ContextNotViableError::what() const {
  return "context not viable";
}

} // namespace channel
} // namespace tensorpipe
