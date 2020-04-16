# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#
# Finduv
# ------
#
# Imported Targets
# ^^^^^^^^^^^^^^^^
#
# An imported target named ``uv::uv`` is provided if libuv has been found.
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# This module defines the following variables:
#
# ``uv_FOUND``
#   True if libuv was found, false otherwise.
# ``uv_LIBRARY_DIRS``
#   The path(s) to uv libraries.
# ``uv_VERSION``
#   The version of libuv found.
#

# If libuv was already added with add_subdirectory() from a parent project,
# simply alias the existing target.
if(TARGET uv_a)
  if(NOT (TARGET uv::uv_a))
    add_library(uv::uv_a ALIAS uv_a)
  endif()
  return()
endif()

find_package(PkgConfig QUIET)

if((NOT TP_BUILD_LIBUV) AND PkgConfig_FOUND)
  pkg_check_modules(uv QUIET libuv)
  if (uv_FOUND)
    add_library(uv::uv_a INTERFACE IMPORTED)

    if(uv_STATIC_INCLUDE_DIRS)
      set_property(TARGET uv::uv_a PROPERTY
                   INTERFACE_INCLUDE_DIRECTORIES "${uv_STATIC_INCLUDE_DIRS}")
    endif()
    if(uv_STATIC_LIBRARIES)
      list(REMOVE_ITEM uv_STATIC_LIBRARIES uv)
      find_library(uv_LIBRARY_ARCHIVE
                   NAMES libuv.a libuv_a.a
                   PATHS "${uv_STATIC_LIBRARY_DIRS}"
                   NO_DEFAULT_PATH)
      list(INSERT uv_STATIC_LIBRARIES 0 "${uv_LIBRARY_ARCHIVE}")
      set_property(TARGET uv::uv_a PROPERTY
                   INTERFACE_LINK_LIBRARIES "${uv_STATIC_LIBRARIES}")
    endif()
    if(uv_STATIC_LDFLAGS_OTHER)
      set_property(TARGET uv::uv_a PROPERTY
                   INTERFACE_LINK_OPTIONS "${uv_STATIC_LDFLAGS_OTHER}")
    endif()
    if(uv_STATIC_CFLAGS_OTHER)
      set_property(TARGET uv::uv_a PROPERTY
                   INTERFACE_COMPILE_OPTIONS "${uv_STATIC_CFLAGS_OTHER}")
    endif()
  endif()
endif()

if(NOT uv_FOUND)
  set(uv_VERSION "1.33.1")
  set(uv_LIBRARY_DIRS "${CMAKE_BINARY_DIR}/third_party/libuv")

  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/libuv
                   ${CMAKE_BINARY_DIR}/third_party/libuv
                   EXCLUDE_FROM_ALL)

  add_library(uv::uv_a ALIAS uv_a)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uv
  REQUIRED_VARS uv_LIBRARY_DIRS
  VERSION_VAR uv_VERSION)
