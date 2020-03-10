# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe_channel_test OBJECT channel_test.cc)
target_include_directories(
  tensorpipe_channel_test
  PUBLIC
  $<TARGET_PROPERTY:tensorpipe,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:uv_a,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:gtest_main,INCLUDE_DIRECTORIES>
  )

function(add_tensorpipe_channel_test)
  add_tensorpipe_test(${ARGV})
  target_link_libraries(
    "${ARGV0}"
    $<TARGET_OBJECTS:tensorpipe_channel_test>
    tensorpipe_uv
    )
endfunction()

add_subdirectory(basic)
