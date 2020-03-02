# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_channel_test(channel_basic_test basic_test.cc)
target_link_libraries(channel_basic_test tensorpipe_basic)
