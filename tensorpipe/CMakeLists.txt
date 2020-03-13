# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(TENSORPIPE_SOURCES
  channel/channel.cc
  channel/basic/basic.cc
  channel/basic/basic.proto
  common/address.cc
  common/error.cc
  common/system.cc
  core/context.cc
  core/core.proto
  core/error.cc
  core/listener.cc
  core/pipe.cc
  transport/connection.cc
  transport/error.cc
  )

if(TP_ENABLE_SHM)
  list(APPEND TENSORPIPE_SOURCES
    transport/shm/context.cc
    transport/shm/connection.cc
    transport/shm/fd.cc
    transport/shm/listener.cc
    transport/shm/loop.cc
    transport/shm/reactor.cc
    transport/shm/socket.cc
    util/ringbuffer/shm.cc
    util/shm/segment.cc
    )
endif()

if(TP_ENABLE_UV)
  list(APPEND TENSORPIPE_SOURCES
    transport/uv/connection.cc
    transport/uv/context.cc
    transport/uv/error.cc
    transport/uv/listener.cc
    transport/uv/loop.cc
    transport/uv/sockaddr.cc
    transport/uv/uv.cc
    )
  list(APPEND TENSORPIPE_DEPENDENCIES uv_a)
endif()

configure_file(config.h.in config.h)

add_library(tensorpipe ${TENSORPIPE_SOURCES})
target_include_directories(tensorpipe PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(tensorpipe PUBLIC ${TENSORPIPE_DEPENDENCIES})

find_package(Protobuf CONFIG REQUIRED)
target_link_libraries(tensorpipe PUBLIC protobuf::libprotobuf)
target_include_directories(tensorpipe PRIVATE ${CMAKE_BINARY_DIR}/tensorpipe)
protobuf_generate(TARGET tensorpipe)

if(TP_BUILD_PYTHON)
  add_subdirectory(python)
endif()

add_subdirectory(benchmark)

if(BUILD_TESTING)
  add_subdirectory(test)
endif()
