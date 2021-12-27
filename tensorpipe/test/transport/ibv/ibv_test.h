/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/test/transport/transport_test.h>
#include <tensorpipe/transport/ibv/factory.h>

class IbvTransportTestHelper : public TransportTestHelper {
 protected:
  std::shared_ptr<tensorpipe::transport::Context> getContextInternal()
      override {
    return tensorpipe::transport::ibv::create();
  }

 public:
  std::string defaultAddr() override {
    return "127.0.0.1";
  }
};
