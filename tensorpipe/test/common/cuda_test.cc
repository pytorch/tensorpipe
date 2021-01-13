/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstring>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/test/peer_group.h>
#include <tensorpipe/test/test_environment.h>

#include <gtest/gtest.h>

namespace {

tensorpipe::CudaLib getCudaLib() {
  tensorpipe::Error error;
  tensorpipe::CudaLib cudaLib;
  std::tie(error, cudaLib) = tensorpipe::CudaLib::create();
  EXPECT_FALSE(error) << error.what();
  return cudaLib;
}

} // namespace

TEST(Cuda, DeviceForPointer) {
  if (TestEnvironment::numCudaDevices() < 2) {
    GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
  }

  {
    ForkedThreadPeerGroup pg;
    pg.spawn(
        [&]() {
          TP_CUDA_CHECK(cudaSetDevice(1));
          void* ptr;
          TP_CUDA_CHECK(cudaMalloc(&ptr, 1024));

          EXPECT_EQ(tensorpipe::cudaFixedDeviceForPointer(getCudaLib(), ptr), 1);

          std::string ptrStr(reinterpret_cast<char*>(&ptr), reinterpret_cast<char*>(&ptr) + sizeof(void*));
          pg.send(PeerGroup::kClient, ptrStr);
        },
        [&]() {
          std::string ptrStr = pg.recv(PeerGroup::kClient);
          void* ptr = *reinterpret_cast<void**>(&ptrStr[0]);

          EXPECT_EQ(tensorpipe::cudaFixedDeviceForPointer(getCudaLib(), ptr), 1);
        });
  }

  {
    ForkedThreadPeerGroup pg;
    pg.spawn(
        [&]() {
          TP_CUDA_CHECK(cudaSetDevice(1));
          void* ptr;
          TP_CUDA_CHECK(cudaMalloc(&ptr, 1024));

          TP_CUDA_CHECK(cudaSetDevice(0));

          EXPECT_EQ(tensorpipe::cudaFixedDeviceForPointer(getCudaLib(), ptr), 1);

          std::string ptrStr(reinterpret_cast<char*>(&ptr), reinterpret_cast<char*>(&ptr) + sizeof(void*));
          pg.send(PeerGroup::kClient, ptrStr);
        },
        [&]() {
          std::string ptrStr = pg.recv(PeerGroup::kClient);
          void* ptr = *reinterpret_cast<void**>(&ptrStr[0]);

          TP_CUDA_CHECK(cudaSetDevice(0));

          EXPECT_EQ(tensorpipe::cudaFixedDeviceForPointer(getCudaLib(), ptr), 1);
        });
  }
}
