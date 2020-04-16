# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(LINUX ON)
else()
  set(LINUX OFF)
endif()

option(TP_ENABLE_SHM "Enable shm transport" ${LINUX})
option(TP_ENABLE_CMA "Enable cma channel" ${LINUX})
option(TP_BUILD_PYTHON "Build python bindings" OFF)
option(TP_BUILD_LIBUV "Build libuv from source" OFF)
