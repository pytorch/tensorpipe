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

#include <tensorpipe/transport/error.h>

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

  // Define template for read callback function that takes a protobuf message.
  //
  // Note: Move semantics for protobuf messages is supported since 3.4
  // (see https://github.com/protocolbuffers/protobuf/issues/2791).
  // We depend on >= 3.0, so use const references.
  //
  template <
      typename T,
      typename std::enable_if<
          std::is_base_of<google::protobuf::MessageLite, T>::value,
          bool>::type = false>
  using read_proto_callback_fn =
      std::function<void(const Error& error, const T& t)>;

  // Read and parse protobuf message.
  //
  // This function may not be overridden by a subclass.
  //
  template <typename T>
  void read(read_proto_callback_fn<T> fn) {
    read([fn{std::move(fn)}](const Error& error, const void* ptr, size_t len) {
      T t;
      if (!error) {
        t.ParseFromArray(ptr, len);
      }
      fn(error, t);
    });
  }

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
