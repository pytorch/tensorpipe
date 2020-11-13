/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>

#include <tensorpipe/transport/connection_boilerplate.h>
#include <tensorpipe/transport/uv/context.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class ConnectionImpl;
class TCPHandle;

class Connection : public ConnectionBoilerplate<ConnectionImpl, ContextImpl> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  Connection(
      ConstructorToken,
      std::shared_ptr<ContextImpl> context,
      std::shared_ptr<TCPHandle> handle,
      std::string id);

  Connection(
      ConstructorToken,
      std::shared_ptr<ContextImpl> context,
      std::string addr,
      std::string id);

  // Allow context to access constructor token.
  friend class Context;
  friend class ContextImpl;
  // Allow listener to access constructor token.
  friend class Listener;
  friend class ListenerImpl;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
