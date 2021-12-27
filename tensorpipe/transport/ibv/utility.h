/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <tuple>

#include <tensorpipe/common/error.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

std::tuple<Error, std::string> lookupAddrForIface(std::string iface);

std::tuple<Error, std::string> lookupAddrForHostname();

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
