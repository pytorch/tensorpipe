/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/error.h>

#include <sstream>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {

const Error Error::kSuccess = Error();

std::string Error::what() const {
  TP_DCHECK(error_);
  return error_->what();
}

} // namespace tensorpipe
