/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <numeric>

#include <tensorpipe/channel/cuda_ipc/factory.h>
#include <tensorpipe/test/channel/channel_test.h>

namespace {

class CudaIpcChannelTestHelper
    : public ChannelTestHelper<tensorpipe::CudaBuffer> {
 protected:
  std::shared_ptr<tensorpipe::channel::CudaContext> makeContextInternal(
      std::string id) override {
    auto context = tensorpipe::channel::cuda_ipc::create();
    context->setId(std::move(id));
    return context;
  }

 public:
  std::shared_ptr<PeerGroup> makePeerGroup() override {
    return std::make_shared<ProcessPeerGroup>();
  }
};

CudaIpcChannelTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(
    CudaIpc,
    CudaChannelTestSuite,
    ::testing::Values(&helper));

INSTANTIATE_TEST_CASE_P(
    CudaIpc,
    CudaMultiGPUChannelTestSuite,
    ::testing::Values(&helper));
