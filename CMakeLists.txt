# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# CMake 3.10 is the version available in Ubuntu 18.04.
cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(tensorpipe C CXX)
set(CMAKE_CXX_STANDARD 14)

option(TP_ENABLE_SHM "Enable shm transport" $<BOOL:LINUX>)

# Use CMAKE_<LANG>_COMPILER_LAUNCHER if available.
find_program(SCCACHE_EXECUTABLE sccache)
mark_as_advanced(SCCACHE_EXECUTABLE)
if(SCCACHE_EXECUTABLE)
  foreach(LANG CXX C)
    if(NOT DEFINED CMAKE_${LANG}_COMPILER_LAUNCHER AND NOT CMAKE_${LANG}_COMPILER MATCHES ".*/s?ccache$")
      message(STATUS "Enabling sccache for ${LANG}")
      set(CMAKE_${LANG}_COMPILER_LAUNCHER ${SCCACHE_EXECUTABLE} CACHE STRING "")
    endif()
  endforeach()
endif()

# Define sanitizer option, if specified.
if(SANITIZE)
  add_definitions("-fsanitize=${SANITIZE}")
  add_definitions("-fno-omit-frame-pointer")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=${SANITIZE}")
endif()

# We use the [[nodiscard]] attribute, which GCC 5 complains about.
# Silence this warning if GCC 5 is used.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6)
    add_definitions("-Wno-attributes")
  endif()
endif()

# CTest must be included in the top level to enable `make test` target.
include(CTest)

# Make dependencies available (but don't build by default)
add_subdirectory(third_party/backward-cpp EXCLUDE_FROM_ALL)
if(BUILD_TESTING)
  add_subdirectory(third_party/googletest EXCLUDE_FROM_ALL)
endif()
add_subdirectory(third_party/libuv EXCLUDE_FROM_ALL)
if(BUILD_PYTHON_MODULE)
  add_subdirectory(third_party/pybind11 EXCLUDE_FROM_ALL)
endif()

# Add include paths for libuv targets.
# These are defined as relative paths in third_party/libuv/CMakeLists.txt,
# but they don't propagate to downstream targets that depend on libuv.
set_target_properties(uv PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${libuv_SOURCE_DIR}/include"
  )
set_target_properties(uv_a PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${libuv_SOURCE_DIR}/include"
  )

add_subdirectory(tensorpipe)
