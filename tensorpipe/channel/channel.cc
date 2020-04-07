/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/channel.h>

namespace tensorpipe {
namespace channel {

Context::Context(std::string name) : name_(std::move(name)) {}

const std::string& Context::name() const {
  return name_;
}

} // namespace channel
} // namespace tensorpipe
