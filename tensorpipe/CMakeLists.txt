# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(TENSORPIPE_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/channel/channel.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/common/address.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/common/error.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/context.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/error.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/connection.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/error.cc
  )

if(LINUX)
  set(TENSORPIPE_SOURCES
    ${TENSORPIPE_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/shm.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/util/shm/segment.cc
  )
endif()

add_subdirectory(proto)
add_library(tensorpipe ${TENSORPIPE_SOURCES})
target_include_directories(tensorpipe PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tensorpipe PUBLIC tensorpipe_proto)

add_subdirectory(channel/basic)

add_subdirectory(transport)

if(BUILD_PYTHON_MODULE)
  add_subdirectory(python)
endif()

add_subdirectory(test)

add_subdirectory(benchmark)
