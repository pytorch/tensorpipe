/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/test/peer_group.h>
#include <tensorpipe/test/test_environment.h>

#include <gtest/gtest.h>

TEST(Cuda, DeviceForPointer) {
  if (TestEnvironment::numCudaDevices() < 2) {
    GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
  }

  ProcessPeerGroup pg;
  pg.spawn(
      [&]() {
        // tensorpipe::Error error;
        // tensorpipe::CudaLib cudaLib;
        // std::tie(error, cudaLib) = tensorpipe::CudaLib::create();
        // ASSERT_FALSE(error) << error.what();

        TP_CUDA_CHECK(cudaSetDevice(1));
        void* ptr;
        TP_CUDA_CHECK(cudaMalloc(&ptr, 1024));
        TP_CUDA_CHECK(cudaSetDevice(0));
        EXPECT_EQ(tensorpipe::cudaDeviceForPointer(ptr), 1);
      },
      [&]() {});
}
