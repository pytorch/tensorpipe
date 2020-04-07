/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/channel/cma/cma.h>
#include <tensorpipe/test/channel/channel_test.h>

class CmaChannelTestHelper : public ChannelTestHelper {
 public:
  std::shared_ptr<tensorpipe::channel::Context> makeContext() override {
    return std::make_shared<tensorpipe::channel::cma::Context>();
  }

  std::string getName() override {
    return "cma";
  }
};
