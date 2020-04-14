/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/transport_test.h>
#include <tensorpipe/transport/uv/context.h>

namespace {

class UVTransportTestHelper : public TransportTestHelper {
 public:
  std::shared_ptr<tensorpipe::transport::Context> getContext() override {
    return std::make_shared<tensorpipe::transport::uv::Context>();
  }

  std::string defaultAddr() override {
    return "127.0.0.1";
  }
};

UVTransportTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(Uv, TransportTest, ::testing::Values(&helper));
