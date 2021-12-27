# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(tensorpipe LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 14)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Expose build options.
include(Options)

# Define sanitizer option, if specified.
include(Sanitize)

# Misc checks to cope with various compiler modes.
include(MiscCheck)

add_subdirectory(tensorpipe)

install(EXPORT TensorpipeTargets
        DESTINATION share/cmake/Tensorpipe
        FILE TensorpipeTargets.cmake)
