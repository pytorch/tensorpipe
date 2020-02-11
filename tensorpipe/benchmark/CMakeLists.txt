# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(benchmark_lib options.cc)

add_executable(benchmark benchmark.cc)
target_link_libraries("benchmark"
  -Wl,--no-whole-archive
  benchmark_lib
  tensorpipe
  tensorpipe_shm
  tensorpipe_uv)
