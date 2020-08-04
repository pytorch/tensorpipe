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
  if(uv_FOUND)
    add_library(uv::uv ALIAS PkgConfig::uv)
  endif()
endif()

if(NOT uv_FOUND)
  set(uv_VERSION "1.38.1")
  set(uv_LIBRARY_DIRS "submodule")

  set(libuv_DIR ${PROJECT_SOURCE_DIR}/third_party/libuv)
  add_subdirectory(${libuv_DIR}
    ${PROJECT_BINARY_DIR}/third_party/libuv)

  # This hack duplicates the `uv_a` target, so that we can call
  # install(TARGETS ... EXPORT) on it, which is not possible when the target is
  # defined in a subdirectory in CMake 3.5.
  get_target_property(_uv_sources uv_a SOURCES)
  set(_uv_sources_abs)
  foreach(_uv_src ${_uv_sources})
    list(APPEND _uv_sources_abs "${libuv_DIR}/${_uv_src}")
  endforeach()

  add_library(_uv_a STATIC ${_uv_sources_abs})
  if(BUILD_SHARED_LIBS)
    set_target_properties(_uv_a PROPERTIES POSITION_INDEPENDENT_CODE 1)
  endif()

  get_target_property(_link_libs uv_a LINK_LIBRARIES)
  target_link_libraries(_uv_a PRIVATE ${_link_libs})

  get_target_property(_include_dirs uv_a INCLUDE_DIRECTORIES)
  target_include_directories(_uv_a PRIVATE ${_include_dirs})
  target_include_directories(_uv_a PUBLIC $<BUILD_INTERFACE:${libuv_DIR}/include>)

  get_target_property(_compile_definitions uv_a COMPILE_DEFINITIONS)
  target_compile_definitions(_uv_a PRIVATE ${_compile_definitions})

  get_target_property(_compile_options uv_a COMPILE_OPTIONS)
  target_compile_options(_uv_a PRIVATE ${_compile_options})

  install(TARGETS _uv_a
          EXPORT TensorpipeTargets
          ARCHIVE DESTINATION ${TP_INSTALL_LIBDIR})

  add_library(uv::uv ALIAS _uv_a)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uv
  REQUIRED_VARS uv_VERSION
  VERSION_VAR uv_VERSION)
