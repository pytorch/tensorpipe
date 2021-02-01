# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe
  ${TP_STATIC_OR_SHARED}
  channel/error.cc
  channel/helpers.cc
  common/address.cc
  common/error.cc
  common/fd.cc
  common/socket.cc
  common/system.cc
  core/context.cc
  core/context_impl.cc
  core/error.cc
  core/listener.cc
  core/listener_impl.cc
  core/pipe.cc
  core/pipe_impl.cc
  transport/error.cc)

# Support `#include <tensorpipe/foo.h>`.
target_include_directories(tensorpipe PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)

# Support `#include <tensorpipe/tensorpipe.h>`.
target_include_directories(tensorpipe PUBLIC $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>)


## Channels

### basic

target_sources(tensorpipe PRIVATE
  channel/basic/channel_impl.cc
  channel/basic/context.cc
  channel/basic/context_impl.cc
  channel/basic/factory.cc)

### xth

target_sources(tensorpipe PRIVATE
  channel/xth/channel_impl.cc
  channel/xth/context.cc
  channel/xth/context_impl.cc
  channel/xth/factory.cc)

### cma

if(TP_ENABLE_CMA)
  target_sources(tensorpipe PRIVATE
    channel/cma/channel_impl.cc
    channel/cma/context.cc
    channel/cma/context_impl.cc
    channel/cma/factory.cc)
  set(TENSORPIPE_HAS_CMA_CHANNEL 1)
else()
  set(TENSORPIPE_HAS_CMA_CHANNEL 0)
endif()

### mpt

target_sources(tensorpipe PRIVATE
  channel/mpt/channel_impl.cc
  channel/mpt/context.cc
  channel/mpt/context_impl.cc
  channel/mpt/factory.cc)

## CUDA channels

if(TP_USE_CUDA)
  find_package(CUDA REQUIRED)
  target_link_libraries(tensorpipe PUBLIC ${CUDA_LIBRARIES})
  target_include_directories(tensorpipe PUBLIC ${CUDA_INCLUDE_DIRS})
  set(TENSORPIPE_SUPPORTS_CUDA 1)

  ### cuda_xth

  target_sources(tensorpipe PRIVATE
    channel/cuda_xth/channel_impl.cc
    channel/cuda_xth/context.cc
    channel/cuda_xth/context_impl.cc
    channel/cuda_xth/factory.cc)

  ### cuda_basic

  target_sources(tensorpipe PRIVATE
    channel/cuda_basic/channel_impl.cc
    channel/cuda_basic/context.cc
    channel/cuda_basic/context_impl.cc
    channel/cuda_basic/factory.cc
    common/cuda_loop.cc)

  ### cuda_ipc

  if(TP_ENABLE_CUDA_IPC)
    target_sources(tensorpipe PRIVATE
      channel/cuda_ipc/channel_impl.cc
      channel/cuda_ipc/context.cc
      channel/cuda_ipc/context_impl.cc
      channel/cuda_ipc/factory.cc)
    set(TENSORPIPE_HAS_CUDA_IPC_CHANNEL 1)
  endif()

  target_sources(tensorpipe PRIVATE
    common/ibv.cc
    channel/cuda_gdr/channel_impl.cc
    channel/cuda_gdr/context.cc
    channel/cuda_gdr/context_impl.cc
    channel/cuda_gdr/factory.cc)
  set(TENSORPIPE_HAS_CUDA_GDR_CHANNEL 1)
endif()

## Transports

### uv

target_sources(tensorpipe PRIVATE
  transport/uv/connection_impl.cc
  transport/uv/context.cc
  transport/uv/context_impl.cc
  transport/uv/error.cc
  transport/uv/factory.cc
  transport/uv/listener_impl.cc
  transport/uv/loop.cc
  transport/uv/sockaddr.cc
  transport/uv/utility.cc)
find_package(uv REQUIRED)
target_link_libraries(tensorpipe PRIVATE uv::uv)

### shm

if(TP_ENABLE_SHM)
  target_sources(tensorpipe PRIVATE
    common/epoll_loop.cc
    transport/shm/connection_impl.cc
    transport/shm/context.cc
    transport/shm/context_impl.cc
    transport/shm/factory.cc
    transport/shm/listener_impl.cc
    transport/shm/reactor.cc
    transport/shm/sockaddr.cc
    util/ringbuffer/shm.cc
    util/shm/segment.cc)
  set(TENSORPIPE_HAS_SHM_TRANSPORT 1)
else()
  set(TENSORPIPE_HAS_SHM_TRANSPORT 0)
endif()

### ibv

if(TP_ENABLE_IBV)
  target_sources(tensorpipe PRIVATE
    common/epoll_loop.cc
    common/ibv.cc
    transport/ibv/connection_impl.cc
    transport/ibv/context.cc
    transport/ibv/context_impl.cc
    transport/ibv/error.cc
    transport/ibv/factory.cc
    transport/ibv/listener_impl.cc
    transport/ibv/reactor.cc
    transport/ibv/sockaddr.cc
    transport/ibv/utility.cc)
  set(TENSORPIPE_HAS_IBV_TRANSPORT 1)
endif()

if(APPLE)
  find_library(CF CoreFoundation)
  find_library(IOKIT IOKit)
  target_link_libraries(tensorpipe PRIVATE ${CF} ${IOKIT})
endif()


if(BUILD_SHARED_LIBS)
  set_target_properties(tensorpipe PROPERTIES POSITION_INDEPENDENT_CODE 1)
endif()


## Config

configure_file(config.h.in config.h)


## Libnop

# We should keep libnop headers private as they should not be exposed to downstream users,
# but they're currently transitively included by tensorpipe/transport/connection.h (which
# is still unclear whether it should be a public or private header).
target_include_directories(tensorpipe PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/third_party/libnop/include>)


## Python bindings

if(TP_BUILD_PYTHON)
  add_subdirectory(python)
endif()


## Benchmarks

if (TP_BUILD_BENCHMARK)
  add_subdirectory(benchmark)
endif()


## Tests

if(TP_BUILD_TESTING)
  add_subdirectory(test)
endif()


## Install

install(TARGETS tensorpipe
        EXPORT TensorpipeTargets
        LIBRARY DESTINATION ${TP_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${TP_INSTALL_LIBDIR})
