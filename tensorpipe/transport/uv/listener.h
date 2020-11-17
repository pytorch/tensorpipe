/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/transport/listener_boilerplate.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class ConnectionImpl;
class ContextImpl;
class ListenerImpl;

using Listener = ListenerBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>;

} // namespace uv
} // namespace transport
} // namespace tensorpipe
