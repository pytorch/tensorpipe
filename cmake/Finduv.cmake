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
  set(uv_LIBRARY_DIRS "submodule")

  add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/libuv
    ${PROJECT_BINARY_DIR}/third_party/libuv)

  add_library(uv::uv ALIAS uv_a)
  set_target_properties(uv_a PROPERTIES POSITION_INDEPENDENT_CODE 1)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uv
  REQUIRED_VARS uv_VERSION
  VERSION_VAR uv_VERSION)
