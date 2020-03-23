# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe_cma cma.cc)
target_link_libraries(tensorpipe_cma tensorpipe)
