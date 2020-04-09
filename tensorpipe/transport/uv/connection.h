/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/defs.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Context;
class Listener;
class Loop;
class Sockaddr;
class TCPHandle;

class Connection : public transport::Connection {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  // Create a connection that is already connected (e.g. from a listener).
  Connection(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  // Create a connection that connects to the specified address.
  Connection(ConstructorToken, std::shared_ptr<Loop> loop, address_t addr);

  using transport::Connection::read_callback_fn;

  void read(read_callback_fn fn) override;

  void read(void* ptr, size_t length, read_callback_fn fn) override;

  using transport::Connection::write_callback_fn;

  void write(const void* ptr, size_t length, write_callback_fn fn) override;

  void close() override;

  ~Connection() override;

 private:
  // All the logic resides in an "implementation" class. The lifetime of these
  // objects is detached from the lifetime of the connection, and is instead
  // attached to the lifetime of the underlying libuv handle. Any operation on
  // these implementation objects must be performed from within the libuv event
  // loop thread, thus all the connection's operations do is schedule the
  // equivalent call on the implementation by deferring to the loop.
  class Impl;

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Impl> impl_;

  // Allow context to access constructor token.
  friend class Context;
  // Allow listener to access constructor token.
  friend class Listener;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
