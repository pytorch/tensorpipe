/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/basic/context.h>
#include <tensorpipe/common/callback.h>

namespace tensorpipe {
namespace channel {
namespace basic {

class Context::PrivateIface {
 public:
  virtual ClosingEmitter& getClosingEmitter() = 0;

  virtual ~PrivateIface() = default;
};

} // namespace basic
} // namespace channel
} // namespace tensorpipe
