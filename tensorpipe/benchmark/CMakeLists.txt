# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_executable(benchmark_transport benchmark_transport.cc options.cc)
target_link_libraries(
  benchmark_transport
  tensorpipe
  tensorpipe_uv
  )

add_executable(benchmark_pipe benchmark_pipe.cc options.cc)
target_link_libraries(
  benchmark_pipe
  tensorpipe
  tensorpipe_uv
  )

if(TP_ENABLE_SHM)
  target_link_libraries(benchmark_transport tensorpipe_shm)
  target_link_libraries(benchmark_pipe tensorpipe_shm)
endif()
