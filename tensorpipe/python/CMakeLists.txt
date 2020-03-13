# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(PYBIND11_CPP_STANDARD -std=c++14)
pybind11_add_module(pytensorpipe tensorpipe.cc)
target_link_libraries(pytensorpipe PRIVATE tensorpipe)
