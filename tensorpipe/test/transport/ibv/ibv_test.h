/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/test/transport/transport_test.h>
#include <tensorpipe/transport/ibv/context.h>

class IbvTransportTestHelper : public TransportTestHelper {
 public:
  std::shared_ptr<tensorpipe::transport::Context> getContext() override {
    return std::make_shared<tensorpipe::transport::ibv::Context>();
  }

  std::string defaultAddr() override {
    return "127.0.0.1";
  }
};
