# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(NOT (COMMAND pybind11_add_module))
  add_subdirectory(
    ${PROJECT_SOURCE_DIR}/third_party/pybind11
    ${PROJECT_BINARY_DIR}/third_party/pybind11
    EXCLUDE_FROM_ALL)
endif()

set(PYBIND11_CPP_STANDARD -std=c++14)
pybind11_add_module(pytensorpipe tensorpipe.cc)
target_link_libraries(pytensorpipe PRIVATE tensorpipe)
