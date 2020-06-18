/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

std::string SystemError::what() const {
  std::ostringstream ss;
  ss << syscall_ << ": " << strerror(error_);
  return ss.str();
}

std::string ShortReadError::what() const {
  std::ostringstream ss;
  ss << "short read: got " << actual_ << " bytes while expecting to read "
     << expected_ << " bytes";
  return ss.str();
}

std::string ShortWriteError::what() const {
  std::ostringstream ss;
  ss << "short write: wrote " << actual_ << " bytes while expecting to write "
     << expected_ << " bytes";
  return ss.str();
}

std::string EOFError::what() const {
  return "eof";
}

std::string ContextClosedError::what() const {
  return "context closed";
}

std::string ChannelClosedError::what() const {
  return "channel closed";
}

} // namespace channel
} // namespace tensorpipe
