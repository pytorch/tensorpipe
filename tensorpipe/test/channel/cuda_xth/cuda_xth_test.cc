/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/cuda_xth/factory.h>
#include <tensorpipe/test/channel/channel_test_cuda.h>

namespace {

class CudaXthChannelTestHelper : public CudaChannelTestHelper {
 protected:
  std::shared_ptr<tensorpipe::channel::Context> makeContextInternal(
      std::string id) override {
    auto context = tensorpipe::channel::cuda_xth::create();
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ForkedThreadPeerGroup>();
  }
};

CudaXthChannelTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(CudaXth, ChannelTestSuite, ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaXth,
    CudaChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaXth,
    CudaMultiGPUChannelTestSuite,
    ::testing::Values(&helper));
