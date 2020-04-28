# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(PYTORCH_BUILD)
  include(cmake/pytorch.cmake)
  return()
endif()

cmake_minimum_required(VERSION 3.17 FATAL_ERROR)

project(tensorpipe
  DESCRIPTION "A tensor-aware point-to-point communication library for machine learning"
  HOMEPAGE_URL "https://github.com/pytorch/tensorpipe"
  LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 14)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Expose build options.
include(Options)

# Define sanitizer option, if specified.
include(Sanitize)

# CTest must be included in the top level to enable `make test` target.
include(CTest)

# We use the [[nodiscard]] attribute, which GCC 5 complains about.
# Silence this warning if GCC 5 is used.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6)
    add_definitions("-Wno-attributes")
  endif()
endif()

add_subdirectory(tensorpipe)
