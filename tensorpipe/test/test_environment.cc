/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/test_environment.h>

#include <tensorpipe/config.h>

#if TENSORPIPE_SUPPORTS_CUDA
#include <cuda_runtime.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <unistd.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

int TestEnvironment::numCudaDevices() {
  static int count = -1;
  if (count == -1) {
#if TENSORPIPE_SUPPORTS_CUDA
    pid_t pid = fork();
    TP_THROW_SYSTEM_IF(pid < 0, errno) << "Failed to fork";
    if (pid == 0) {
      int res;
      TP_CUDA_CHECK(cudaGetDeviceCount(&res));
      std::exit(res);
    } else {
      int status;
      TP_THROW_SYSTEM_IF(waitpid(pid, &status, 0) < 0, errno)
          << "Failed to wait for child process";
      TP_THROW_ASSERT_IF(!WIFEXITED(status));
      count = WEXITSTATUS(status);
    }
#else // TENSORPIPE_SUPPORTS_CUDA
    count = 0;
#endif // TENSORPIPE_SUPPORTS_CUDA
  }

  return count;
}
