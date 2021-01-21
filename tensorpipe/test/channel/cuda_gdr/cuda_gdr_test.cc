/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/cuda_gdr/context.h>
#include <tensorpipe/test/channel/channel_test.h>

namespace {

class CudaGdrChannelTestHelper
    : public ChannelTestHelper<tensorpipe::CudaBuffer> {
 protected:
  std::shared_ptr<tensorpipe::channel::CudaContext> makeContextInternal(
      std::string id) override {
    auto context = std::make_shared<tensorpipe::channel::cuda_gdr::Context>();
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ProcessPeerGroup>();
  }
};

CudaGdrChannelTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(
    CudaGdr,
    CudaChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaGdr,
    CudaMultiGPUChannelTestSuite,
    ::testing::Values(&helper));
