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
  Connection(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  ~Connection() override;

  using transport::Connection::read_callback_fn;

  void read(read_callback_fn fn) override;

  void read(void* ptr, size_t length, read_callback_fn fn) override;

  using transport::Connection::write_callback_fn;

  void write(const void* ptr, size_t length, write_callback_fn fn) override;

 private:
  // Create a connection that connects to the specified address.
  static std::shared_ptr<Connection> create_(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  // Create a connection that is already connected (e.g. from a listener).
  static std::shared_ptr<Connection> create_(
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  // All the logic resides in an "implementation" class. The lifetime of these
  // objects is detached from the lifetime of the connection, and is instead
  // attached to the lifetime of the underlying libuv handle. Any operation on
  // these implementation objects must be performed from within the libuv event
  // loop thread, thus all the connection's operations do is schedule the
  // equivalent call on the implementation by deferring to the loop.
  class Impl;

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Impl> impl_;

  friend class Context;
  friend class Listener;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
