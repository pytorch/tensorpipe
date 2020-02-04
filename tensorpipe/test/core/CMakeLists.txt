# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_tensorpipe_test(core_context_test context_test.cc)
target_link_libraries(core_context_test tensorpipe)
target_link_libraries(core_context_test tensorpipe_shm)
target_link_libraries(core_context_test tensorpipe_uv)

add_tensorpipe_test(core_channel_test channel_test.cc)
target_link_libraries(core_context_test tensorpipe)
