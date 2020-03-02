# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_test(segment_test segment_test.cc)
# Needed to use socket.h to send fds across a UNIX domain socket.
target_link_libraries(segment_test tensorpipe_shm)
