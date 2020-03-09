# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(
  tensorpipe_transport_test
  OBJECT
  connection_test.cc
  context_test.cc
  listener_test.cc
  )
target_link_libraries(tensorpipe_transport_test tensorpipe_proto)
target_include_directories(
  tensorpipe_transport_test
  PUBLIC
  $<TARGET_PROPERTY:tensorpipe,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:gtest_main,INCLUDE_DIRECTORIES>
  )

function(add_tensorpipe_transport_test)
  add_tensorpipe_test(${ARGV})
  target_link_libraries(
    "${ARGV0}"
    $<TARGET_OBJECTS:tensorpipe_transport_test>
    )
endfunction()

add_subdirectory(uv)
if(LINUX)
  add_subdirectory(shm)
endif()
