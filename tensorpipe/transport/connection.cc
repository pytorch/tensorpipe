/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/connection.h>

#include <google/protobuf/message_lite.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {

void Connection::read(
    google::protobuf::MessageLite& message,
    read_proto_callback_fn fn) {
  read([&message, fn{std::move(fn)}](
           const Error& error, const void* ptr, size_t len) {
    if (!error) {
      message.ParseFromArray(ptr, len);
    }
    fn(error);
  });
}

void Connection::write(
    const google::protobuf::MessageLite& message,
    write_callback_fn fn) {
  // FIXME use ByteSizeLong (introduced in a newer protobuf).
  const auto len = message.ByteSize();

  // Using a shared_ptr instead of unique_ptr because if the lambda captures a
  // unique_ptr then it becomes non-copyable, which prevents it from being
  // converted to a function. In C++20 use std::make_shared<uint8_t[]>(len).
  //
  // Note: this is a std::shared_ptr<uint8_t[]> semantically. A shared_ptr
  // with array type is supported in C++17 and higher.
  //
  auto buf = std::shared_ptr<uint8_t>(
      new uint8_t[len], std::default_delete<uint8_t[]>());
  auto ptr = buf.get();
  auto end = message.SerializeWithCachedSizesToArray(ptr);
  TP_DCHECK_EQ(end, ptr + len) << "Failed to serialize protobuf message.";

  // Perform write and forward callback.
  write(
      ptr,
      len,
      [buf{std::move(buf)}, fn{std::move(fn)}](const Error& error) mutable {
        // The write has completed; destroy write buffer.
        buf.reset();
        fn(error);
      });
}

} // namespace transport
} // namespace tensorpipe
