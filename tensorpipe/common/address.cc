/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/address.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {

std::tuple<std::string, std::string> splitSchemeOfAddress(
    const std::string& addr) {
  std::string::size_type endOfScheme = addr.find("://");
  if (endOfScheme == std::string::npos) {
    TP_THROW_EINVAL() << "addr has no scheme: " << addr;
  }
  return std::make_tuple(
      addr.substr(0, endOfScheme), addr.substr(endOfScheme + 3));
}

} // namespace tensorpipe
