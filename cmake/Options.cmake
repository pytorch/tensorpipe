# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(LINUX ON)
else()
  set(LINUX OFF)
endif()

macro(TP_CONDITIONAL_BACKEND name docstring condition)
  # No clue why this monstrosity is needed. But cmake_dependent_option has it,
  # and the code doesn't seem to work without it.
  string(REGEX REPLACE " +" ";" TP_CONDITIONAL_BACKEND_CONDITION "${condition}")
  if(${TP_CONDITIONAL_BACKEND_CONDITION})
    set(TP_CONDITIONAL_BACKEND_CAN_ENABLE ON)
  else()
    set(TP_CONDITIONAL_BACKEND_CAN_ENABLE OFF)
  endif()
  set(${name} ${TP_CONDITIONAL_BACKEND_CAN_ENABLE} CACHE BOOL ${docstring})
  if(${name} AND NOT ${TP_CONDITIONAL_BACKEND_CAN_ENABLE})
    message(FATAL_ERROR "${name} was explicitly set, but that can't be honored")
  endif()
endmacro()

# Try to auto-detect the presence of some libraries in order to enable/disable
# the transports/channels that make use of them.
# TODO Add CUDA to this list, in order to fix the TODO below

# TODO: Default to ON if CUDA available.
option(TP_USE_CUDA "Enable support for CUDA tensors" OFF)

# Optional features
option(TP_BUILD_BENCHMARK "Build benchmarks" OFF)
option(TP_BUILD_MISC "Build misc tools" OFF)
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
set(TP_INSTALL_INCLUDEDIR ${CMAKE_INSTALL_INCLUDEDIR} CACHE STRING "Directory in which to install public headers")
mark_as_advanced(TP_INSTALL_INCLUDEDIR)
