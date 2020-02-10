# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe_shm context.cc connection.cc fd.cc listener.cc loop.cc reactor.cc socket.cc)
target_link_libraries(tensorpipe_shm tensorpipe)
