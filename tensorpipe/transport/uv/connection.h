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
 public:
  Connection(
      std::shared_ptr<ContextImpl> context,
      std::shared_ptr<TCPHandle> handle,
      std::string id);

  Connection(
      std::shared_ptr<ContextImpl> context,
      std::string addr,
      std::string id);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
