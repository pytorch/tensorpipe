# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_package(uv REQUIRED)

add_library(tensorpipe_uv connection.cc context.cc error.cc listener.cc loop.cc sockaddr.cc uv.cc)
target_link_libraries(tensorpipe_uv PUBLIC tensorpipe uv::uv_a)
