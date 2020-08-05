# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(tensorpipe LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 14)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Enable protobuf_generate() polyfill.
include(cmake/ProtobufGenerate.cmake)

# Expose build options.
include(Options)

# Define sanitizer option, if specified.
include(Sanitize)

# We use the [[nodiscard]] attribute, which GCC 5 complains about.
# Silence this warning if GCC 5 is used.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6)
    add_definitions("-Wno-attributes")
  endif()
endif()

add_subdirectory(tensorpipe)

install(EXPORT TensorpipeTargets
        DESTINATION share/cmake/Tensorpipe
        FILE TensorpipeTargets.cmake)
