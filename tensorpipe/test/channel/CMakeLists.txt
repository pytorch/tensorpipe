# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_test(tensorpipe_channel_test
  channel_test.cc
  basic/basic_test.cc
  )
target_link_libraries(tensorpipe_channel_test tensorpipe_basic tensorpipe_uv)
