/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/factory.h>

#include <tensorpipe/transport/ibv/context.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

// Make namespaces explicit to disambiguate the downcast.
std::shared_ptr<transport::Context> create() {
  return std::make_shared<ibv::Context>();
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
