# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_test(transport_shm_test
  connection_test.cc
  context_test.cc
  listener_test.cc
  loop_test.cc
  reactor_test.cc
  shm_test.cc
  sockaddr_test.cc
  )
target_link_libraries(transport_shm_test tensorpipe_shm)
