/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <string>

#include <tensorpipe/common/error.h>
#include <tensorpipe/common/nop.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {

class Connection {
 public:
  using read_callback_fn =
      std::function<void(const Error& error, const void* ptr, size_t length)>;

  virtual void read(read_callback_fn fn) = 0;

  virtual void read(void* ptr, size_t length, read_callback_fn fn) = 0;

  using write_callback_fn = std::function<void(const Error& error)>;

  virtual void write(const void* ptr, size_t length, write_callback_fn fn) = 0;

  //
  // Helper functions for reading/writing nop objects.
  //

  // Read and parse a nop object.
  //
  // This function may be overridden by a subclass.
  //
  // For example, the shm transport may be able to bypass reading into a
  // temporary buffer and instead instead read directly from its peer's
  // ring buffer. This saves an allocation and a memory copy.
  //
  using read_nop_callback_fn = std::function<void(const Error& error)>;

  virtual void read(AbstractNopHolder& object, read_nop_callback_fn fn) = 0;

  // Serialize and write nop object.
  //
  // This function may be overridden by a subclass.
  //
  // For example, the shm transport may be able to bypass serialization
  // into a temporary buffer and instead instead serialize directly into
  // its peer's ring buffer. This saves an allocation and a memory copy.
  //
  virtual void write(const AbstractNopHolder& object, write_callback_fn fn) = 0;

  // Tell the connection what its identifier is.
  //
  // This is only supposed to be called from the high-level pipe or from
  // channels. It will only used for logging and debugging purposes.
  virtual void setId(std::string id) = 0;

  virtual void close() = 0;

  virtual ~Connection() = default;
};

} // namespace transport
} // namespace tensorpipe
