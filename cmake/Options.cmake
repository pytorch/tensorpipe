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

# Transports
option(TP_ENABLE_SHM "Enable shm transport" ${LINUX})

# Channels
option(TP_ENABLE_CMA "Enable cma channel" ${LINUX})

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
