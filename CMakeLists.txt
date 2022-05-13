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
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cmake/Tensorpipe
        FILE TensorpipeTargets.cmake)

# Create TensorpipeConfig.cmake for find_package(Tensorpipe CONFIG)
include(CMakePackageConfigHelpers)
get_filename_component(CONFIG_FILE_PATH ${CMAKE_CURRENT_BINARY_DIR}/TensorpipeConfig.cmake ABSOLUTE)
configure_package_config_file(
  cmake/TensorpipeConfig.cmake.in ${CONFIG_FILE_PATH}
  INSTALL_DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cmake/Tensorpipe)

# Install the generated config file
install(FILES ${CONFIG_FILE_PATH}
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/cmake/Tensorpipe)
