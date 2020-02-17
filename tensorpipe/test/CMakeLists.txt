# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe_testing STATIC test.cc)
target_link_libraries(tensorpipe_testing PUBLIC gtest_main)

function(add_tensorpipe_test)
  add_executable(${ARGV})
  target_link_libraries("${ARGV0}"
    -Wl,--whole-archive
    tensorpipe_testing
    -Wl,--no-whole-archive
    tensorpipe
    tensorpipe_shm
    tensorpipe_uv
    tensorpipe_basic)
endfunction()

add_subdirectory(channel)
add_subdirectory(channel/basic)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(proto)
add_subdirectory(transport/shm)
add_subdirectory(transport/uv)
add_subdirectory(util/ringbuffer)
add_subdirectory(util/shm)
