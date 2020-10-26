/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>

#include <tensorpipe/channel/cma/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error.h>

namespace tensorpipe {
namespace channel {
namespace cma {

class Context::PrivateIface {
 public:
  virtual ClosingEmitter& getClosingEmitter() = 0;

  using copy_request_callback_fn = std::function<void(const Error&)>;

  virtual void requestCopy(
      pid_t remotePid,
      void* remotePtr,
      void* localPtr,
      size_t length,
      copy_request_callback_fn fn) = 0;

  virtual ~PrivateIface() = default;
};

} // namespace cma
} // namespace channel
} // namespace tensorpipe
