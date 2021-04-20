# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# cmake file to trigger hipify

set(HIPIFY_SCRIPTS_DIR ${PROJECT_SOURCE_DIR}/tools/amd_build)
set(HIPIFY_COMMAND
  ${HIPIFY_SCRIPTS_DIR}/build_amd.py
  --project-directory ${PROJECT_SOURCE_DIR}
  --output-directory ${PROJECT_SOURCE_DIR}
)

execute_process(
  COMMAND ${HIPIFY_COMMAND}
  RESULT_VARIABLE hipify_return_value
)
if (NOT hipify_return_value EQUAL 0)
  message(FATAL_ERROR "Failed to hipify files!")
endif()

