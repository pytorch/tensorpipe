# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TP_SRCS is the list of source files that we need to build libtensorpipe.
set(TP_TEST_SRCS)
set(TP_TEST_CUDA_SRCS)
set(TP_TEST_ROCM_SRCS)

# TP_HDRS is the list of header files that we need to install.
set(TP_TEST_HDRS)
set(TP_TEST_CUDA_HDRS)
set(TP_TEST_ROCM_HDRS)

list(APPEND TP_TEST_SRCS
  "${CMAKE_CURRENT_SOURCE_DIR}/test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/test_environment.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/context_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/connection_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/uv_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/context_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/loop_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/connection_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/sockaddr_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/transport/listener_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/core/context_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/basic/basic_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/xth/xth_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/mpt/mpt_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/channel_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/channel/channel_test_cpu.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/system_test.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/common/defs_test.cc"
  )

if(TP_ENABLE_SHM)
  list(APPEND TP_TEST_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/common/epoll_loop_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/ringbuffer_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/shm_ringbuffer_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/shm_segment_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/reactor_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/connection_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/sockaddr_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/shm/shm_test.cc"
    )
endif()

if(TP_ENABLE_IBV)
  list(APPEND TP_TEST_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/common/epoll_loop_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/ringbuffer_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/connection_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/ibv_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/transport/ibv/sockaddr_test.cc"
    )
endif()

if(TP_ENABLE_CMA)
  list(APPEND TP_TEST_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cma/cma_test.cc"
    )
  add_subdirectory(channel/cma)
endif()

if(TP_USE_CUDA)
  list(APPEND TP_TEST_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/channel_test_cuda.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/channel_test_cuda_multi_gpu.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/cuda_test.cc"
    )
  
  list(APPEND TP_TEST_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_xth/cuda_xth_test.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_basic/cuda_basic_test.cc"
    )

  if(TP_ENABLE_CUDA_IPC)
    list(APPEND TP_TEST_SRCS
      "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_ipc/cuda_ipc_test.cc"
      )
  endif()

  list(APPEND TP_TEST_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/cuda_gdr/cuda_gdr_test.cc"
    )

  list(APPEND TP_TEST_CUDA_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/channel/kernel.cu"
    )
endif()

add_executable(tensorpipe_test ${TP_TEST_SRCS})

if(TP_USE_CUDA)
  find_package(CUDA REQUIRED)
  target_link_libraries(tensorpipe_test PRIVATE ${CUDA_LIBRARIES})
  target_include_directories(tensorpipe_test PRIVATE ${CUDA_INCLUDE_DIRS})

  cuda_add_library(tensorpipe_cuda_kernel ${TP_TEST_CUDA_SRCS})
  target_link_libraries(tensorpipe_test PRIVATE tensorpipe_cuda_kernel)
endif()

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/googletest
  ${PROJECT_BINARY_DIR}/third_party/googletest)

target_link_libraries(tensorpipe_test PRIVATE
  tensorpipe
  uv::uv
  gmock
  gtest_main)
