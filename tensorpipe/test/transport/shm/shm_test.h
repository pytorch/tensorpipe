/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>

#include <tensorpipe/test/transport/transport_test.h>
#include <tensorpipe/transport/shm/context.h>

class SHMTransportTestHelper : public TransportTestHelper {
 public:
  std::shared_ptr<tensorpipe::transport::Context> getContext() override {
    return std::make_shared<tensorpipe::transport::shm::Context>();
  }

  std::string defaultAddr() override {
    const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    std::ostringstream ss;
    // Once we upgrade googletest, also use test_info->test_suite_name() here.
    ss << "tensorpipe_test_" << test_info->name() << "_" << getpid();
    return ss.str();
  }
};
