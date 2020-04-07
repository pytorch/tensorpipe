# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe)

# Support `#include <tensorpipe/foo.h>`.
target_include_directories(tensorpipe PUBLIC
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
  )

target_sources(tensorpipe PRIVATE
  channel/context.cc
  channel/error.cc
  common/address.cc
  common/error.cc
  common/system.cc
  core/context.cc
  core/error.cc
  core/listener.cc
  core/pipe.cc
  transport/connection.cc
  transport/error.cc
  )


## Protobufs

add_subdirectory(proto)
target_link_libraries(tensorpipe PUBLIC tensorpipe_proto)


## Channels

### basic

target_sources(tensorpipe PRIVATE
  channel/basic/channel.cc
  channel/basic/context.cc)

### cma

if(TP_ENABLE_CMA)
  target_sources(tensorpipe PRIVATE
    channel/cma/channel.cc
    channel/cma/context.cc)
  target_compile_definitions(tensorpipe INTERFACE TP_ENABLE_CMA)
endif()


## Transports

### uv

find_package(uv REQUIRED)

target_sources(tensorpipe PRIVATE
  transport/uv/connection.cc
  transport/uv/context.cc
  transport/uv/error.cc
  transport/uv/listener.cc
  transport/uv/loop.cc
  transport/uv/sockaddr.cc
  transport/uv/uv.cc
  )
target_link_libraries(tensorpipe PRIVATE uv::uv_a)

### shm

if(TP_ENABLE_SHM)
  target_sources(tensorpipe PRIVATE
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
  target_compile_definitions(tensorpipe INTERFACE TP_ENABLE_SHM)
endif()


# Python bindings

if(TP_BUILD_PYTHON)
  add_subdirectory(
    ${PROJECT_SOURCE_DIR}/third_party/pybind11
    ${PROJECT_BINARY_DIR}/third_party/pybind11
    EXCLUDE_FROM_ALL)
  add_subdirectory(python)
endif()


# Benchmarks

add_subdirectory(benchmark)


# Tests

if(BUILD_TESTING)
  add_subdirectory(test)
endif()
