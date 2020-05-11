/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/error.h>

#include <cstring>
#include <sstream>

namespace tensorpipe {
namespace transport {

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

std::string ListenerClosedError::what() const {
  return "listener closed";
}

std::string ConnectionClosedError::what() const {
  return "connection closed";
}

} // namespace transport
} // namespace tensorpipe
