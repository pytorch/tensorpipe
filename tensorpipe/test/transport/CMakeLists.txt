# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_test(transport_test
  connection_test.cc
  )
target_link_libraries(transport_test tensorpipe_shm tensorpipe_uv)
