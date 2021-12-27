# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_executable(dump_state_machine dump_state_machine.cc)
find_package(Clang REQUIRED)
target_include_directories(dump_state_machine PRIVATE ${CLANG_INCLUDE_DIRS})
target_link_libraries(dump_state_machine PRIVATE
  clangTooling
  clangBasic
  clangASTMatchers)
