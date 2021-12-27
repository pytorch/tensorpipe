# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_executable(tensorpipe_channel_cma_probe
  probe.cc
)

target_link_libraries(tensorpipe_channel_cma_probe PRIVATE
  tensorpipe
)
