# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_transport_test(
  transport_uv_test
  connection_test.cc
  loop_test.cc
  sockaddr_test.cc
  uv_test.cc
  )
target_link_libraries(transport_uv_test tensorpipe_uv)
