/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/efa/factory.h>

#include <tensorpipe/transport/context_boilerplate.h>
#include <tensorpipe/transport/efa/connection_impl.h>
#include <tensorpipe/transport/efa/context_impl.h>
#include <tensorpipe/transport/efa/listener_impl.h>

namespace tensorpipe {
namespace transport {
namespace efa {

std::shared_ptr<Context> create() {
  return std::make_shared<
      ContextBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>>();
}

} // namespace efa
} // namespace transport
} // namespace tensorpipe
