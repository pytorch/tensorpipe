/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/error.h>

#include <cstring>
#include <sstream>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {

const Error Error::kSuccess = Error();

std::string Error::what() const {
  TP_DCHECK(error_);
  std::ostringstream ss;
  ss << error_->what() << " (this error originated at " << file_ << ":" << line_
     << ")";
  return ss.str();
}

std::string SystemError::what() const {
  std::ostringstream ss;
  ss << syscall_ << ": " << strerror(error_);
  return ss.str();
}

int SystemError::errorCode() const {
  return error_;
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

} // namespace tensorpipe
