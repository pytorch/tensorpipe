# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Defines `gtest_discover_tests()`.
include(GoogleTest)

add_library(tensorpipe_test OBJECT test.cc)

function(add_tensorpipe_test)
  add_executable(${ARGV})
  target_link_libraries(
    "${ARGV0}"
    $<TARGET_OBJECTS:tensorpipe_test>
    tensorpipe
    gtest_main
    )
  gtest_discover_tests(${ARGV0})
endfunction()

add_subdirectory(channel)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(proto)
add_subdirectory(transport)
add_subdirectory(util/ringbuffer)
if(LINUX)
  add_subdirectory(util/shm)
endif()
