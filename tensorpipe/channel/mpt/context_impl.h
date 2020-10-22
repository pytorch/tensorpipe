/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <tensorpipe/channel/mpt/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

class Context::PrivateIface {
 public:
  virtual ClosingEmitter& getClosingEmitter() = 0;

  using connection_request_callback_fn =
      std::function<void(const Error&, std::shared_ptr<transport::Connection>)>;

  virtual const std::vector<std::string>& addresses() const = 0;

  virtual uint64_t registerConnectionRequest(
      uint64_t laneIdx,
      connection_request_callback_fn) = 0;

  virtual void unregisterConnectionRequest(uint64_t) = 0;

  virtual std::shared_ptr<transport::Connection> connect(
      uint64_t laneIdx,
      std::string address) = 0;

  virtual ~PrivateIface() = default;
};

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
