# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_test(tensorpipe_transport_test
  connection_test.cc
  context_test.cc
  listener_test.cc
  uv/connection_test.cc
  uv/loop_test.cc
  uv/sockaddr_test.cc
  uv/uv_test.cc
  )
target_link_libraries(tensorpipe_transport_test tensorpipe_uv)

if(LINUX)
  target_sources(tensorpipe_transport_test
    PRIVATE
    shm/connection_test.cc
    shm/loop_test.cc
    shm/reactor_test.cc
    shm/shm_test.cc
    shm/sockaddr_test.cc
    )
  target_link_libraries(tensorpipe_transport_test tensorpipe_shm)
endif()
