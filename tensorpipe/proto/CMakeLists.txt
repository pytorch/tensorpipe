# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_package(Protobuf 3 REQUIRED)
protobuf_generate_cpp(
  PROTOBUF_SOURCES
  PROTOBUF_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/core.proto
  )
add_library(tensorpipe_proto ${PROTOBUF_SOURCES})
target_include_directories(tensorpipe_proto PUBLIC ${CMAKE_BINARY_DIR})
target_include_directories(tensorpipe_proto PUBLIC ${Protobuf_INCLUDE_DIRS})
target_link_libraries(tensorpipe_proto PUBLIC ${Protobuf_LIBRARIES})

# We need to have a CMakeLists.txt file in each subdirectory in order to
# preserve the hierarchy, because CMake's protobuf tools flatten the tree.
# See https://gitlab.kitware.com/cmake/cmake/issues/18828.
add_subdirectory(channel)
target_link_libraries(tensorpipe_proto PUBLIC tensorpipe_proto_channel)
