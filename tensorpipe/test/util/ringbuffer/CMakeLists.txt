# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_test(ringbuffer_test ringbuffer_test.cc)
add_tensorpipe_test(protobuf_streams_test protobuf_streams_test.cc)
target_link_libraries(protobuf_streams_test tensorpipe_proto)

if(TP_ENABLE_SHM)
  add_tensorpipe_test(shm_ringbuffer_test shm_ringbuffer_test.cc)
  # Needed to use socket.h to send fds across a UNIX domain socket.
  target_link_libraries(shm_ringbuffer_test tensorpipe_shm)
endif()
