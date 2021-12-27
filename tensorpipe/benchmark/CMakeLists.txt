# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TODO: Make those separate CMake projects.

add_executable(benchmark_transport benchmark_transport.cc options.cc transport_registry.cc)
target_link_libraries(benchmark_transport PRIVATE tensorpipe)

add_executable(benchmark_pipe benchmark_pipe.cc options.cc transport_registry.cc channel_registry.cc)
target_link_libraries(benchmark_pipe PRIVATE tensorpipe tensorpipe_cuda)
