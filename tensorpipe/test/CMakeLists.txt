# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe_test OBJECT test.cc)

function(add_tensorpipe_test)
  add_executable(${ARGV})
  target_link_libraries(
    "${ARGV0}"
    $<TARGET_OBJECTS:tensorpipe_test>
    tensorpipe
    gtest_main
    )
endfunction()

add_subdirectory(channel/basic)
add_subdirectory(channel/process_vm_readv)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(proto)
add_subdirectory(transport)
add_subdirectory(util/ringbuffer)
add_subdirectory(util/shm)
