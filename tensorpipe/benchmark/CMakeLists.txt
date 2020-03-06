# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TODO: Re-enable on all platforms once it does not explicitly rely on shm which
#   is Linux only.
if(LINUX)
  add_executable(benchmark_transport benchmark_transport.cc options.cc)
  target_link_libraries(
    benchmark_transport
    tensorpipe
    tensorpipe_shm
    tensorpipe_uv
    )

  add_executable(benchmark_pipe benchmark_pipe.cc options.cc)
  target_link_libraries(
    benchmark_pipe
    tensorpipe
    tensorpipe_shm
    tensorpipe_uv
    )
endif()
