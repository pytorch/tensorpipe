# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TODO: Make those separate CMake projects.

set(TP_BM_TRANS_SRCS)

set(TP_BM_PIPE_SRCS)

list(APPEND TP_BM_TRANS_SRCS
  benchmark_transport
  benchmark_transport.cc
  options.cc
  transport_registry.cc
  channel_registry.cc)

list(APPEND TP_BM_PIPE_SRCS
  benchmark_pipe.cc
  options.cc
  transport_registry.cc
  channel_registry.cc)

if(TP_USE_ROCM)
  # Replace the cuda file names in TP_BM_TRANS_SRCS & TP_BM_PIPE_SRCS file lists with hipified file names
  get_hipified_list("${TP_BM_TRANS_SRCS}" TP_BM_TRANS_SRCS)
  get_hipified_list("${TP_BM_PIPE_SRCS}" TP_BM_PIPE_SRCS)
  set(TP_GPU_LIB "tensorpipe_hip")
elseif(TP_USE_CUDA)
  set(TP_GPU_LIB "tensorpipe_cuda")
endif()

add_executable(benchmark_transport ${TP_BM_TRANS_SRCS})
target_link_libraries(benchmark_transport PRIVATE tensorpipe)

add_executable(benchmark_pipe ${TP_BM_PIPE_SRCS})
target_link_libraries(benchmark_pipe PRIVATE tensorpipe ${TP_GPU_LIB})
