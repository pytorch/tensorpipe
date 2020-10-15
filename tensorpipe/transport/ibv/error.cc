/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/error.h>

#include <tensorpipe/common/ibv.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

std::string IbvError::what() const {
  return ibv_wc_status_str(status_);
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
