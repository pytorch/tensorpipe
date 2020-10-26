/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <tensorpipe/common/error.h>

namespace tensorpipe {
namespace transport {

// FIXME There used to be an EOFError specific to transports but it got merged
// into the global one. We're keeping this alias because PyTorch is explcitly
// using the one from the transport namespace. However, PyTorch should be fixed
// and this alias should be removed.
using EOFError = ::tensorpipe::EOFError;

class ListenerClosedError final : public BaseError {
 public:
  ListenerClosedError() {}

  std::string what() const override;
};

class ConnectionClosedError final : public BaseError {
 public:
  ConnectionClosedError() {}

  std::string what() const override;
};

} // namespace transport
} // namespace tensorpipe
