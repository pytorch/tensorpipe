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

find_package(PkgConfig QUIET)

if((NOT TP_BUILD_LIBUV) AND PkgConfig_FOUND)
  pkg_check_modules(uv QUIET IMPORTED_TARGET GLOBAL libuv)
  add_library(uv::uv ALIAS PkgConfig::uv)
endif()

if(NOT uv_FOUND)
  set(uv_VERSION "1.37.0")
  set(uv_LIBRARY_DIRS "downloaded")

  include(FetchContent)
  FetchContent_Declare(libuv
    URL "https://github.com/libuv/libuv/archive/v${uv_VERSION}.tar.gz")
  FetchContent_MakeAvailable(libuv)

  add_library(uv::uv ALIAS uv_a)

  install(TARGETS uv_a
    EXPORT libuv-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR})
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uv
  REQUIRED_VARS uv_LIBRARY_DIRS
  VERSION_VAR uv_VERSION)
