/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstring>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/test/peer_group.h>
#include <tensorpipe/test/test_environment.h>

#include <gtest/gtest.h>

namespace {

tensorpipe::DynamicLibraryHandle getCudaLibHandle() {
  tensorpipe::Error error;
  tensorpipe::DynamicLibraryHandle handle;
  std::tie(error, handle) = tensorpipe::loadCuda();
  EXPECT_FALSE(error) << error.what();
  return handle;
}

} // namespace

// This tests whether we can retrieve the index of the device on which a pointer
// resides under "normal" circumstances (in the same context where it was
// allocated, or in a "fresh" thread).
TEST(Cuda, DeviceForPointer) {
  if (TestEnvironment::numCudaDevices() < 2) {
    GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
  }

  ForkedThreadPeerGroup pg;
  pg.spawn(
      [&]() {
        auto cudaLibHandle = getCudaLibHandle();
        TP_CUDA_CHECK(cudaSetDevice(1));
        void* ptr;
        TP_CUDA_CHECK(cudaMalloc(&ptr, 1024));

        EXPECT_EQ(tensorpipe::cudaDeviceForPointer(ptr), 1);

        std::string ptrStr(
            reinterpret_cast<char*>(&ptr),
            reinterpret_cast<char*>(&ptr) + sizeof(void*));
        pg.send(PeerGroup::kClient, ptrStr);
      },
      [&]() {
        auto cudaLibHandle = getCudaLibHandle();
        std::string ptrStr = pg.recv(PeerGroup::kClient);
        void* ptr = *reinterpret_cast<void**>(&ptrStr[0]);

        EXPECT_EQ(tensorpipe::cudaDeviceForPointer(ptr), 1);
      });
}

// This tests whether we can retrieve the index of the device on which a pointer
// resided after we've explicitly set the current device to an invalid value.
// This is known to cause problems in recent versions of CUDA, possibly because
// of a bug.
TEST(Cuda, DeviceForPointerAfterReset) {
  if (TestEnvironment::numCudaDevices() < 2) {
    GTEST_SKIP() << "Skipping test requiring >=2 CUDA devices.";
  }

  ForkedThreadPeerGroup pg;
  pg.spawn(
      [&]() {
        auto cudaLibHandle = getCudaLibHandle();
        TP_CUDA_CHECK(cudaSetDevice(1));
        void* ptr;
        TP_CUDA_CHECK(cudaMalloc(&ptr, 1024));

        TP_CUDA_CHECK(cudaSetDevice(0));

        EXPECT_EQ(tensorpipe::cudaDeviceForPointer(ptr), 1);

        std::string ptrStr(
            reinterpret_cast<char*>(&ptr),
            reinterpret_cast<char*>(&ptr) + sizeof(void*));
        pg.send(PeerGroup::kClient, ptrStr);
      },
      [&]() {
        auto cudaLibHandle = getCudaLibHandle();
        std::string ptrStr = pg.recv(PeerGroup::kClient);
        void* ptr = *reinterpret_cast<void**>(&ptrStr[0]);

        TP_CUDA_CHECK(cudaSetDevice(0));

        EXPECT_EQ(tensorpipe::cudaDeviceForPointer(ptr), 1);
      });
}
