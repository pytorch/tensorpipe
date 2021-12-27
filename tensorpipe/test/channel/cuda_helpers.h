/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cstdlib>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_lib.h>
#include <tensorpipe/common/nvml_lib.h>

namespace tensorpipe {

inline bool isContextOpenOnDevice(const NvmlLib& nvmlLib, nvmlDevice_t device) {
  unsigned int count = 0;
  std::vector<nvmlProcessInfo_t> processInfos;
  while (true) {
    nvmlReturn_t res = nvmlLib.deviceGetComputeRunningProcesses(
        device, &count, processInfos.data());
    processInfos.resize(count);
    if (res == NVML_SUCCESS) {
      break;
    }
    if (res == NVML_ERROR_INSUFFICIENT_SIZE) {
      continue;
    }
    TP_NVML_CHECK(nvmlLib, res);
  }

  pid_t myPid = ::getpid();
  for (const nvmlProcessInfo_t& processInfo : processInfos) {
    if (processInfo.pid == myPid) {
      return true;
    }
  }
  return false;
}

inline ::testing::AssertionResult initializedCudaContexts(
    const std::vector<int>& expectedDeviceIndices) {
  // This check won't work when the test is running in a PID namespace, as NVML
  // will return the PIDs in the root namespace but it doesn't seem possible for
  // us to map them back to our namespace. Hence we use an env var to allow to
  // disable this check in such environments.
  char* shouldSkip = std::getenv("TP_SKIP_CHECK_OPEN_CUDA_CTXS");
  if (shouldSkip != nullptr) {
    return ::testing::AssertionSuccess();
  }

  Error error;
  CudaLib cudaLib;
  std::tie(error, cudaLib) = CudaLib::create();
  TP_THROW_ASSERT_IF(error) << error.what();
  NvmlLib nvmlLib;
  std::tie(error, nvmlLib) = NvmlLib::create();
  TP_THROW_ASSERT_IF(error) << error.what();

  std::vector<std::string> uuids = getUuidsOfVisibleDevices(cudaLib);
  for (int deviceIdx = 0; deviceIdx < uuids.size(); deviceIdx++) {
    // NVML uses a different format for UUIDs.
    std::string nvmlUuid = "GPU-" + uuids[deviceIdx];
    nvmlDevice_t nvmlDevice;
    TP_NVML_CHECK(
        nvmlLib, nvmlLib.deviceGetHandleByUUID(nvmlUuid.c_str(), &nvmlDevice));
    bool actualHasCtx = isContextOpenOnDevice(nvmlLib, nvmlDevice);

    bool expectedHasCtx = std::find(
                              expectedDeviceIndices.begin(),
                              expectedDeviceIndices.end(),
                              deviceIdx) != expectedDeviceIndices.end();

    if (actualHasCtx && !expectedHasCtx) {
      return ::testing::AssertionFailure()
          << "a CUDA context was initialized on device #" << deviceIdx
          << " but that shouldn't have happened";
    }
    if (!actualHasCtx && expectedHasCtx) {
      return ::testing::AssertionFailure()
          << "a CUDA context should have been initialized on device #"
          << deviceIdx << " but that didn't happen";
    }
  }
  return ::testing::AssertionSuccess();
}

} // namespace tensorpipe
