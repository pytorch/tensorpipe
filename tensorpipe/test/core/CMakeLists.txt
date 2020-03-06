# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TODO: Re-enable this once it does not rely on shm which is Linux only.
if(LINUX)
  add_tensorpipe_test(core_context_test context_test.cc)
  target_link_libraries(
    core_context_test
    tensorpipe
    tensorpipe_shm
    tensorpipe_uv
    tensorpipe_basic
  )
endif()
