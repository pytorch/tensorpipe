# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_channel_test(channel_cma_test cma_test.cc)
target_link_libraries(channel_cma_test tensorpipe_cma)
