# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(tensorpipe_cma channel.cc context.cc)
target_link_libraries(tensorpipe_cma tensorpipe)
target_compile_definitions(tensorpipe_cma INTERFACE TP_ENABLE_CMA)
