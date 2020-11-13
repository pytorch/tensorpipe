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
 public:
  // Create a listener that listens on the specified address.
  Listener(
      std::shared_ptr<ContextImpl> context,
      std::string addr,
      std::string id);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
