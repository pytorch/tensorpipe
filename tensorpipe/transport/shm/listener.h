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
#include <tensorpipe/transport/shm/context.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Context;
class Loop;
class Sockaddr;

class Listener final : public transport::Listener {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  // Create a listener that listens on the specified address.
  Listener(
      ConstructorToken,
      std::shared_ptr<Context::PrivateIface> context,
      address_t addr,
      std::string id);

  // Queue a callback to be called when a connection comes in.
  void accept(accept_callback_fn fn) override;

  // Obtain the listener's address.
  address_t addr() const override;

  // Tell the listener what its identifier is.
  void setId(std::string id) override;

  // Shut down the connection and its resources.
  void close() override;

  ~Listener() override;

 private:
  class Impl;

  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  std::shared_ptr<Impl> impl_;

  // Allow context to access constructor token.
  friend class Context;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
