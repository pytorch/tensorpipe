# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_package(Protobuf 3 REQUIRED)
protobuf_generate_cpp(
  PROTOBUF_SOURCES
  PROTOBUF_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/all.proto
  )
add_library(tensorpipe_proto ${PROTOBUF_SOURCES})
target_include_directories(tensorpipe_proto PUBLIC ${CMAKE_BINARY_DIR})
target_include_directories(tensorpipe_proto PUBLIC ${Protobuf_INCLUDE_DIRS})
target_link_libraries(tensorpipe_proto PRIVATE ${Protobuf_LIBRARIES})
