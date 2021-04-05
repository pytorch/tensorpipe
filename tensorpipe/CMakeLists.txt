# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TP_SRCS is the list of source files that we need to build libtensorpipe.
set(TP_SRCS)
set(TP_CUDA_SRCS)
set(TP_ROCM_SRCS)

# TP_HDRS is the list of header files that we need to install.
set(TP_HDRS)
set(TP_CUDA_HDRS)
set(TP_ROCM_HDRS)

list(APPEND TP_SRCS
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/error.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/helpers.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/address.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/allocator.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/error.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/fd.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/socket.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/system.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/context.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/context_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/error.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/listener.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/listener_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/pipe_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/error.cc"
  )

list(APPEND TP_HDRS
  "${CMAKE_CURRENT_SOURCE_DIR}/tensorpipe.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/context.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/error.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/buffer.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/cpu_buffer.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/device.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/error.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/optional.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/context.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/error.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/listener.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/message.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/context.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/error.h"
  )

## Channels

### basic

list(APPEND TP_SRCS
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/basic/channel_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/basic/context_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/basic/factory.cc"
  )
list(APPEND TP_HDRS
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/basic/factory.h")

### xth

list(APPEND TP_SRCS
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/xth/channel_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/xth/context_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/xth/factory.cc"
  )
list(APPEND TP_HDRS
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/xth/factory.h")

### cma

tp_conditional_backend(
  TP_ENABLE_CMA "Enable cross-memory attach channel" "LINUX AND SUPPORT_GLIBCXX_USE_PROCESS_VM_READV")
if(TP_ENABLE_CMA)
  list(APPEND TP_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cma/channel_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cma/context_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cma/factory.cc"
    )
  list(APPEND TP_HDRS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cma/factory.h")
  set(TENSORPIPE_HAS_CMA_CHANNEL 1)
endif()

### mpt

list(APPEND TP_SRCS
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/mpt/channel_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/mpt/context_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/mpt/factory.cc"
  )
list(APPEND TP_HDRS
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/mpt/factory.h")

## CUDA channels

if(TP_USE_CUDA)
  list(APPEND TP_CUDA_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/common/cuda_buffer.cc")
  list(APPEND TP_CUDA_HDRS
    "${CMAKE_CURRENT_SOURCE_DIR}/common/cuda_buffer.h")
  set(TENSORPIPE_SUPPORTS_CUDA 1)

  ### cuda_xth

  list(APPEND TP_CUDA_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_xth/channel_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_xth/context_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_xth/factory.cc"
    )
  list(APPEND TP_CUDA_HDRS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_xth/factory.h")

  ### cuda_basic

  list(APPEND TP_CUDA_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_basic/channel_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_basic/context_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_basic/factory.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/cuda_loop.cc"
    )
  list(APPEND TP_CUDA_HDRS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_basic/factory.h")

  ### cuda_ipc

  tp_conditional_backend(
    TP_ENABLE_CUDA_IPC "Enable CUDA inter-process communication channel" "TP_USE_CUDA")
  if(TP_ENABLE_CUDA_IPC)
    list(APPEND TP_CUDA_SRCS
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_ipc/channel_impl.cc"
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_ipc/context_impl.cc"
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_ipc/factory.cc"
      )
    list(APPEND TP_CUDA_HDRS
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_ipc/factory.h")
    set(TENSORPIPE_HAS_CUDA_IPC_CHANNEL 1)
  endif()

  ### cuda_gdr

  tp_conditional_backend(
    TP_ENABLE_CUDA_GDR "Enable CUDA GpuDirect (InfiniBand) channel" "LINUX")
  if(TP_ENABLE_CUDA_GDR)
    list(APPEND TP_CUDA_SRCS
      "${CMAKE_CURRENT_SOURCE_DIR}/common/ibv.cc"
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_gdr/channel_impl.cc"
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_gdr/context_impl.cc"
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_gdr/factory.cc"
      )
    list(APPEND TP_CUDA_HDRS
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_gdr/error.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_gdr/factory.h"
      )
    set(TENSORPIPE_HAS_CUDA_GDR_CHANNEL 1)
  endif()
endif()

## Transports

### uv

list(APPEND TP_SRCS
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/connection_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/context_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/error.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/factory.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/listener_impl.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/loop.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/sockaddr.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/utility.cc"
  )
list(APPEND TP_HDRS
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/error.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/factory.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/utility.h"
  )

### shm

tp_conditional_backend(
  TP_ENABLE_SHM "Enable shared-memory transport" "LINUX")
if(TP_ENABLE_SHM)
  list(APPEND TP_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/common/epoll_loop.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/shm_segment.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/connection_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/context_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/factory.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/listener_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/reactor.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/sockaddr.cc"
    )
  list(APPEND TP_HDRS
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/factory.h")
  set(TENSORPIPE_HAS_SHM_TRANSPORT 1)
endif()

### ibv

tp_conditional_backend(
  TP_ENABLE_IBV "Enable InfiniBand transport" "LINUX")
if(TP_ENABLE_IBV)
  list(APPEND TP_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/common/epoll_loop.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/ibv.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/connection_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/context_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/error.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/factory.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/listener_impl.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/reactor.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/sockaddr.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/utility.cc"
    )
  list(APPEND TP_HDRS
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/error.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/factory.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/utility.h"
    )
  set(TENSORPIPE_HAS_IBV_TRANSPORT 1)
endif()

if(TP_USE_CUDA)
  list(APPEND TP_SRCS ${TP_CUDA_SRCS})
  list(APPEND TP_HDRS ${TP_CUDA_HDRS})
endif()

add_library(tensorpipe ${TP_STATIC_OR_SHARED} ${TP_SRCS})

find_package(uv REQUIRED)
target_link_libraries(tensorpipe PRIVATE uv::uv)

if(TP_USE_CUDA)
  find_package(CUDA REQUIRED)
  target_link_libraries(tensorpipe PUBLIC ${CUDA_LIBRARIES})
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


## Add include directories to the target

target_include_directories(tensorpipe PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
  $<INSTALL_INTERFACE:${TP_INSTALL_INCLUDEDIR}>)
if(TP_USE_CUDA)
  target_include_directories(tensorpipe PUBLIC ${CUDA_INCLUDE_DIRS})
endif()

## Libnop

# We should keep libnop headers private as they should not be exposed to downstream users,
# but they're currently transitively included by tensorpipe/transport/connection.h (which
# is still unclear whether it should be a public or private header).
target_include_directories(tensorpipe PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/third_party/libnop/include>)

## Install

install(TARGETS tensorpipe
        EXPORT TensorpipeTargets
        LIBRARY DESTINATION ${TP_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${TP_INSTALL_LIBDIR})

foreach(_header_file ${TP_HDRS})
  get_filename_component(_TP_HEADER_SUBDIR "${_header_file}" DIRECTORY)
  install(FILES ${_header_file}
          DESTINATION ${TP_INSTALL_INCLUDEDIR}/tensorpipe/${_TP_HEADER_SUBDIR})
endforeach()

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/config.h
        DESTINATION ${TP_INSTALL_INCLUDEDIR}/tensorpipe)
