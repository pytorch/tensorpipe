# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TODO: Make those separate CMake projects.

add_executable(benchmark_transport benchmark_transport.cc options.cc)
target_link_libraries(benchmark_transport PRIVATE -Wl,--whole-archive tensorpipe -Wl,--no-whole-archive)

add_executable(benchmark_pipe benchmark_pipe.cc options.cc)
target_link_libraries(benchmark_pipe PRIVATE -Wl,--whole-archive tensorpipe -Wl,--no-whole-archive)
