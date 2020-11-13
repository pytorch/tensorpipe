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

#include <tensorpipe/transport/listener_boilerplate.h>
#include <tensorpipe/transport/uv/context.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class ListenerImpl;

class Listener : public ListenerBoilerplate<ListenerImpl, ContextImpl> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  // Create a listener that listens on the specified address.
  Listener(
      ConstructorToken,
      std::shared_ptr<ContextImpl> context,
      std::string addr,
      std::string id);

  // Allow context to access constructor token.
  friend class Context;
  friend class ContextImpl;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
