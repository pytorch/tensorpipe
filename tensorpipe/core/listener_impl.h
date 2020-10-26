/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>

#include <tensorpipe/common/error.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class Listener::PrivateIface {
 public:
  using connection_request_callback_fn = std::function<
      void(const Error&, std::string, std::shared_ptr<transport::Connection>)>;

  virtual uint64_t registerConnectionRequest(
      connection_request_callback_fn) = 0;

  virtual void unregisterConnectionRequest(uint64_t) = 0;

  virtual const std::map<std::string, std::string>& addresses() const = 0;

  virtual ~PrivateIface() = default;
};

} // namespace tensorpipe
