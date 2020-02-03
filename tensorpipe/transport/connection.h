/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>

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
};

} // namespace transport
} // namespace tensorpipe
