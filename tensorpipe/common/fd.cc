/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/fd.h>

#include <unistd.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {

ssize_t Fd::read(void* buf, size_t count) {
  ssize_t rv = -1;
  for (;;) {
    rv = ::read(fd_, buf, count);
    if (rv == -1 && errno == EINTR) {
      continue;
    }
    break;
  }
  return rv;
}

// Proxy to write(2) with EINTR retry.
ssize_t Fd::write(const void* buf, size_t count) {
  ssize_t rv = -1;
  for (;;) {
    rv = ::write(fd_, buf, count);
    if (rv == -1 && errno == EINTR) {
      continue;
    }
    break;
  }
  return rv;
}

// Call read and throw if it doesn't complete.
Error Fd::readFull(void* buf, size_t count) {
  auto rv = read(buf, count);
  if (rv == -1) {
    return TP_CREATE_ERROR(SystemError, "read", errno);
  }
  if (rv != count) {
    return TP_CREATE_ERROR(ShortReadError, count, rv);
  }
  return Error::kSuccess;
}

// Call write and throw if it doesn't complete.
Error Fd::writeFull(const void* buf, size_t count) {
  auto rv = write(buf, count);
  if (rv == -1) {
    return TP_CREATE_ERROR(SystemError, "write", errno);
  }
  if (rv != count) {
    return TP_CREATE_ERROR(ShortWriteError, count, rv);
  }
  return Error::kSuccess;
}

} // namespace tensorpipe
