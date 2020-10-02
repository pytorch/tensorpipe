# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(LINUX ON)
else()
  set(LINUX OFF)
endif()

include(CMakeDependentOption)

# Try to auto-detect the presence of some libraries in order to enable/disable
# the transports/channels that make use of them.
# TODO Add CUDA to this list, in order to fix the TODO below
find_package(ibverbs QUIET)

# TODO: Default to ON if CUDA available.
option(TP_USE_CUDA "Enable support for CUDA tensors" OFF)

# Transports
if (${LINUX} AND ${IBVERBS_FOUND})
  set(TP_ENABLE_IBV_DEFAULT ON)
else()
  set(TP_ENABLE_IBV_DEFAULT OFF)
endif()
option(TP_ENABLE_IBV "Enable InfiniBand transport" ${TP_ENABLE_IBV_DEFAULT})
unset(TP_ENABLE_IBV_DEFAULT)
option(TP_ENABLE_SHM "Enable shm transport" ${LINUX})

# Channels
option(TP_ENABLE_CMA "Enable cma channel" ${LINUX})
cmake_dependent_option(TP_ENABLE_CUDA_IPC "Enable CUDA IPC channel" ON
                       "TP_USE_CUDA" OFF)

# Optional features
option(TP_BUILD_BENCHMARK "Build benchmarks" OFF)
option(TP_BUILD_PYTHON "Build python bindings" OFF)
option(TP_BUILD_TESTING "Build tests" OFF)

# Whether to build a static or shared library
if(BUILD_SHARED_LIBS)
  set(TP_STATIC_OR_SHARED SHARED CACHE STRING "")
else()
  set(TP_STATIC_OR_SHARED STATIC CACHE STRING "")
endif()
mark_as_advanced(TP_STATIC_OR_SHARED)

# Force to build libuv from the included submodule
option(TP_BUILD_LIBUV "Build libuv from source" OFF)

# Directories
include(GNUInstallDirs)
set(TP_INSTALL_LIBDIR ${CMAKE_INSTALL_LIBDIR} CACHE STRING "Directory in which to install libraries")
mark_as_advanced(TP_INSTALL_LIBDIR)
