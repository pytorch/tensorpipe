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

set(TENSORPIPE_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/channel/channel.h
  ${CMAKE_CURRENT_SOURCE_DIR}/channel/helpers.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/address.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/callback.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/defs.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/error.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/error_macros.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/optional.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/queue.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/context.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/error.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/message.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/connection.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/context.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/error.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/listener.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/consumer.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/producer.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/protobuf_streams.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/ringbuffer.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/shm.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/shm/segment.h
  )

add_subdirectory(proto)
add_library(tensorpipe ${TENSORPIPE_SOURCES})
target_include_directories(tensorpipe PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tensorpipe PUBLIC tensorpipe_proto)

add_subdirectory(channel/basic)
add_subdirectory(transport/uv)
if(LINUX)
  add_subdirectory(transport/shm)
endif()

if(BUILD_PYTHON_MODULE)
  add_subdirectory(python)
endif()

add_subdirectory(benchmark)

if(BUILD_TESTING)
  add_subdirectory(test)
endif()
