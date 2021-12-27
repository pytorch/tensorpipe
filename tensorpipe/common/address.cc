/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/address.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {

std::tuple<std::string, std::string> splitSchemeOfURL(const std::string& url) {
  std::string::size_type endOfScheme = url.find("://");
  if (endOfScheme == std::string::npos) {
    TP_THROW_EINVAL() << "url has no scheme: " << url;
  }
  return std::make_tuple(
      url.substr(0, endOfScheme), url.substr(endOfScheme + 3));
}

} // namespace tensorpipe
