# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# List of source files that we need to build libtensorpipe.
set(TP_PUBLIC_SRCS)
set(TP_PRIVATE_SRCS)
set(TP_CUDA_PRIVATE_SRCS)

# List of header files that we need to install.
set(TP_PUBLIC_HDRS)
set(TP_CUDA_HDRS)

list(APPEND TP_PUBLIC_SRCS
  channel/error.cc
  channel/helpers.cc
  common/address.cc
  common/allocator.cc
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

list(APPEND TP_PUBLIC_HDRS
  tensorpipe.h
  channel/context.h
  channel/error.h
  common/buffer.h
  common/cpu_buffer.h
  common/device.h
  common/error.h
  common/optional.h
  core/context.h
  core/error.h
  core/listener.h
  core/message.h
  core/pipe.h
  transport/context.h
  transport/error.h)


## Channels

### basic

list(APPEND TP_PRIVATE_SRCS
  channel/basic/channel_impl.cc
  channel/basic/context_impl.cc
  channel/basic/factory.cc)
list(APPEND TP_PUBLIC_HDRS
  channel/basic/factory.h)

### xth
list(APPEND TP_PRIVATE_SRCS
  channel/xth/channel_impl.cc
  channel/xth/context_impl.cc
  channel/xth/factory.cc)
list(APPEND TP_PUBLIC_HDRS
  channel/xth/factory.h)
### cma

tp_conditional_backend(
  TP_ENABLE_CMA "Enable cross-memory attach channel" "LINUX")
if(TP_ENABLE_CMA)
  list(APPEND TP_PRIVATE_SRCS
    channel/cma/channel_impl.cc
    channel/cma/context_impl.cc
    channel/cma/factory.cc)
  list(APPEND TP_PUBLIC_HDRS
    channel/cma/factory.h)
  set(TENSORPIPE_HAS_CMA_CHANNEL 1)
endif()

### mpt

list(APPEND TP_PRIVATE_SRCS
  channel/mpt/channel_impl.cc
  channel/mpt/context_impl.cc
  channel/mpt/factory.cc)
list(APPEND TP_PUBLIC_HDRS
  channel/mpt/factory.h)

## CUDA channels

if(TP_USE_CUDA)
  list(APPEND TP_CUDA_PRIVATE_SRCS
    common/cuda_buffer.cc)
  list(APPEND TP_CUDA_HDRS
    common/cuda_buffer.h)
  set(TENSORPIPE_SUPPORTS_CUDA 1)

  ### cuda_xth

  list(APPEND TP_CUDA_PRIVATE_SRCS
    channel/cuda_xth/channel_impl.cc
    channel/cuda_xth/context_impl.cc
    channel/cuda_xth/factory.cc)
  list(APPEND TP_CUDA_HDRS
    channel/cuda_xth/factory.h)

  ### cuda_basic

  list(APPEND TP_CUDA_PRIVATE_SRCS
    channel/cuda_basic/channel_impl.cc
    channel/cuda_basic/context_impl.cc
    channel/cuda_basic/factory.cc
    common/cuda_loop.cc)
  list(APPEND TP_CUDA_HDRS
    channel/cuda_basic/factory.h)

  ### cuda_ipc

  tp_conditional_backend(
    TP_ENABLE_CUDA_IPC "Enable CUDA inter-process communication channel" "TP_USE_CUDA")
  if(TP_ENABLE_CUDA_IPC)
    list(APPEND TP_CUDA_PRIVATE_SRCS
      channel/cuda_ipc/channel_impl.cc
      channel/cuda_ipc/context_impl.cc
      channel/cuda_ipc/factory.cc)
    list(APPEND TP_CUDA_HDRS
      channel/cuda_ipc/factory.h)
    set(TENSORPIPE_HAS_CUDA_IPC_CHANNEL 1)
  endif()

  ### cuda_gdr

  tp_conditional_backend(
    TP_ENABLE_CUDA_GDR "Enable CUDA GpuDirect (InfiniBand) channel" "LINUX")
  if(TP_ENABLE_CUDA_GDR)
    list(APPEND TP_CUDA_PRIVATE_SRCS
      common/ibv.cc
      channel/cuda_gdr/channel_impl.cc
      channel/cuda_gdr/context_impl.cc
      channel/cuda_gdr/factory.cc)
    list(APPEND TP_CUDA_HDRS
      channel/cuda_gdr/error.h
      channel/cuda_gdr/factory.h)
    set(TENSORPIPE_HAS_CUDA_GDR_CHANNEL 1)
  endif()
endif()

## Transports

### uv

list(APPEND TP_PRIVATE_SRCS
  transport/uv/connection_impl.cc
  transport/uv/context_impl.cc
  transport/uv/error.cc
  transport/uv/factory.cc
  transport/uv/listener_impl.cc
  transport/uv/loop.cc
  transport/uv/sockaddr.cc
  transport/uv/utility.cc)
list(APPEND TP_PUBLIC_HDRS
  transport/uv/error.h
  transport/uv/factory.h
  transport/uv/utility.h)

### shm

tp_conditional_backend(
  TP_ENABLE_SHM "Enable shared-memory transport" "LINUX")
if(TP_ENABLE_SHM)
  list(APPEND TP_PRIVATE_SRCS
    common/epoll_loop.cc
    common/shm_segment.cc
    transport/shm/connection_impl.cc
    transport/shm/context_impl.cc
    transport/shm/factory.cc
    transport/shm/listener_impl.cc
    transport/shm/reactor.cc
    transport/shm/sockaddr.cc)
  list(APPEND TP_PUBLIC_HDRS
    transport/shm/factory.h)
  set(TENSORPIPE_HAS_SHM_TRANSPORT 1)
endif()

### ibv

tp_conditional_backend(
  TP_ENABLE_IBV "Enable InfiniBand transport" "LINUX")
if(TP_ENABLE_IBV)
  list(APPEND TP_PRIVATE_SRCS
    common/epoll_loop.cc
    common/ibv.cc
    transport/ibv/connection_impl.cc
    transport/ibv/context_impl.cc
    transport/ibv/error.cc
    transport/ibv/factory.cc
    transport/ibv/listener_impl.cc
    transport/ibv/reactor.cc
    transport/ibv/sockaddr.cc
    transport/ibv/utility.cc)
  list(APPEND TP_PUBLIC_HDRS
    transport/ibv/error.h
    transport/ibv/factory.h
    transport/ibv/utility.h)
  set(TENSORPIPE_HAS_IBV_TRANSPORT 1)
endif()

# Handling the file lists
if(TP_USE_CUDA)
  list(APPEND TP_PRIVATE_SRCS ${TP_CUDA_PRIVATE_SRCS})
  list(APPEND TP_PUBLIC_HDRS ${TP_CUDA_HDRS})
endif()

# Add the library target
add_library(tensorpipe ${TP_STATIC_OR_SHARED} ${TP_PUBLIC_SRCS})
target_sources(tensorpipe PRIVATE ${TP_PRIVATE_SRCS})

# Set target properties
if(BUILD_SHARED_LIBS)
  set_target_properties(tensorpipe PROPERTIES POSITION_INDEPENDENT_CODE 1)
endif()

# Add uv package
find_package(uv REQUIRED)
target_link_libraries(tensorpipe PRIVATE uv::uv)

# If CUDA is required find the CUDA package and add to link deps
if(TP_USE_CUDA)
  find_package(CUDA REQUIRED)
  target_link_libraries(tensorpipe PUBLIC ${CUDA_LIBRARIES})
  target_include_directories(tensorpipe PUBLIC ${CUDA_INCLUDE_DIRS})
endif()

# MAC OS specific library deps
if(APPLE)
  find_library(CF CoreFoundation)
  find_library(IOKIT IOKit)
  target_link_libraries(tensorpipe PRIVATE ${CF} ${IOKIT})
endif()


## Config

configure_file(config.h.in config.h)

target_include_directories(tensorpipe PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
  $<INSTALL_INTERFACE:${TP_INSTALL_INCLUDEDIR}>)


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


## Misc tools

if (TP_BUILD_MISC)
  add_subdirectory(misc)
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

foreach(_header_file ${TP_PUBLIC_HDRS})
  get_filename_component(_TP_HEADER_SUBDIR "${_header_file}" DIRECTORY)
  install(FILES ${_header_file}
          DESTINATION ${TP_INSTALL_INCLUDEDIR}/tensorpipe/${_TP_HEADER_SUBDIR})
endforeach()

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/config.h
        DESTINATION ${TP_INSTALL_INCLUDEDIR}/tensorpipe)
