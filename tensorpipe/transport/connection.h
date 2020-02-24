/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>

#include <google/protobuf/message_lite.h>

#include <tensorpipe/common/error.h>

namespace tensorpipe {
namespace transport {

class Connection {
 public:
  virtual ~Connection() = default;

  using read_callback_fn =
      std::function<void(const Error& error, const void* ptr, size_t len)>;

  virtual void read(read_callback_fn fn) = 0;

  virtual void read(void* ptr, size_t length, read_callback_fn fn) = 0;

  using write_callback_fn = std::function<void(const Error& error)>;

  virtual void write(const void* ptr, size_t length, write_callback_fn fn) = 0;

  //
  // Helper functions for reading/writing protobuf messages.
  //

  // Read and parse protobuf message.
  //
  // This function may not be overridden by a subclass.
  //
  using read_proto_callback_fn = std::function<void(const Error& error)>;

  void read(google::protobuf::MessageLite& message, read_proto_callback_fn fn);

  // Serialize and write protobuf message.
  //
  // This function may be overridden by a subclass.
  //
  // For example, the shm transport may be able to bypass serialization
  // into a temporary buffer and instead instead serialize directly into
  // its peer's ring buffer. This saves an allocation and a memory copy.
  //
  virtual void write(
      const google::protobuf::MessageLite& message,
      write_callback_fn fn);
};

} // namespace transport
} // namespace tensorpipe
