# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(TEST_SOURCES
  transport/context_test.cc
  transport/connection_test.cc
  transport/listener_test.cc
  core/context_test.cc
  core/proto_test.cc
  util/ringbuffer/protobuf_streams_test.cc
  util/ringbuffer/ringbuffer_test.cc
  channel/basic/basic_test.cc
  channel/channel_test.cc
  common/system_test.cc
  common/defs_test.cc
  test.cc
  )

if(TP_ENABLE_SHM)
  list(APPEND TEST_SOURCES
    transport/shm/reactor_test.cc
    transport/shm/loop_test.cc
    transport/shm/connection_test.cc
    transport/shm/sockaddr_test.cc
    transport/shm/shm_test.cc
    util/ringbuffer/shm_ringbuffer_test.cc
    util/shm/segment_test.cc
    )
endif()

if(TP_ENABLE_UV)
  list(APPEND TEST_SOURCES
    transport/uv/uv_test.cc
    transport/uv/loop_test.cc
    transport/uv/connection_test.cc
    transport/uv/sockaddr_test.cc
    )
endif()

add_executable(tensorpipe_test ${TEST_SOURCES})
target_link_libraries(tensorpipe_test tensorpipe gtest_main)

include(GoogleTest)
gtest_discover_tests(tensorpipe_test)
