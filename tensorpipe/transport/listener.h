/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>

#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {

class Listener {
 public:
  using accept_callback_fn = std::function<
      void(const Error& error, std::shared_ptr<Connection> connection)>;

  virtual void accept(accept_callback_fn fn) = 0;

  // Return address that this listener is listening on.
  // This may be required if the listening address is not known up
  // front, or dynamically populated by the operating system (e.g. by
  // letting the operating system pick a TCP port to listen on).
  virtual std::string addr() const = 0;

  // Tell the listener what its identifier is.
  //
  // This is only supposed to be called from the high-level listener or from
  // channel contexts. It will only used for logging and debugging purposes.
  virtual void setId(std::string id) = 0;

  virtual void close() = 0;

  virtual ~Listener() = default;
};

} // namespace transport
} // namespace tensorpipe
