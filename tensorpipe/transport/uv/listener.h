/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Context;
class Loop;
class Sockaddr;
class TCPHandle;

class Listener : public transport::Listener {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  Listener(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  using transport::Listener::accept_callback_fn;

  void accept(accept_callback_fn fn) override;

  address_t addr() const override;

  void close() override;

  ~Listener() override;

 private:
  // Create a listener that listens on the specified address.
  static std::shared_ptr<Listener> create_(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  // All the logic resides in an "implementation" class. The lifetime of these
  // objects is detached from the lifetime of the listener, and is instead
  // attached to the lifetime of the underlying libuv handle. Any operation on
  // these implementation objects must be performed from within the libuv event
  // loop thread, thus all the listeners's operations do is schedule the
  // equivalent call on the implementation by deferring to the loop.
  class Impl;

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Impl> impl_;

  friend class Context;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
