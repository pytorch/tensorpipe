# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# cmake file to trigger hipify

function(write_file_list FILE_SUFFIX INPUT_LIST)
  message(STATUS "Writing ${FILE_SUFFIX} into file - file_${FILE_SUFFIX}.txt")
  set(_FULL_FILE_NAME "${CMAKE_BINARY_DIR}/cuda_to_hip_list_${FILE_SUFFIX}.txt")
  file(WRITE ${_FULL_FILE_NAME} "")
  foreach(_SOURCE_FILE ${INPUT_LIST})
    file(APPEND ${_FULL_FILE_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/${_SOURCE_FILE})
    file(APPEND ${_FULL_FILE_NAME} "\n")
  endforeach()
endfunction()

function(get_file_list FILE_SUFFIX OUTPUT_LIST)
  set(_FULL_FILE_NAME "${CMAKE_BINARY_DIR}/cuda_to_hip_list_${FILE_SUFFIX}.txt")
  file(STRINGS ${_FULL_FILE_NAME} _FILE_LIST)
  set(${OUTPUT_LIST}_HIP ${_FILE_LIST} PARENT_SCOPE)
endfunction()

function(update_list_with_hip_files FILE_SUFFIX)
  set(_SCRIPTS_DIR ${PROJECT_SOURCE_DIR}/tools/amd_build)
  set(_FULL_FILE_NAME "${CMAKE_BINARY_DIR}/cuda_to_hip_list_${FILE_SUFFIX}.txt")
  set(_EXE_COMMAND
    ${_SCRIPTS_DIR}/replace_cuda_with_hip_files.py
    --io-file ${_FULL_FILE_NAME}
    --dump-dict-directory ${CMAKE_BINARY_DIR})
  execute_process(
    COMMAND ${_EXE_COMMAND}
    RESULT_VARIABLE _return_value)
  if (NOT _return_value EQUAL 0)
    message(FATAL_ERROR "Failed to get the list of hipified files!")
  endif()
endfunction()

function(get_hipified_list FILE_SUFFIX INPUT_LIST OUTPUT_LIST)
  write_file_list("${FILE_SUFFIX}" "${INPUT_LIST}")
  update_list_with_hip_files("${FILE_SUFFIX}")
  get_file_list("${FILE_SUFFIX}" __temp_srcs)
  set(${OUTPUT_LIST} ${__temp_srcs_HIP} PARENT_SCOPE)
endfunction()


set(HIPIFY_SCRIPTS_DIR ${PROJECT_SOURCE_DIR}/tools/amd_build)
set(HIPIFY_COMMAND
  ${HIPIFY_SCRIPTS_DIR}/build_amd.py
  --project-directory ${PROJECT_SOURCE_DIR}
  --output-directory ${PROJECT_SOURCE_DIR}
  --dump-dict-directory ${CMAKE_BINARY_DIR}
)

execute_process(
  COMMAND ${HIPIFY_COMMAND}
  RESULT_VARIABLE hipify_return_value
)
if (NOT hipify_return_value EQUAL 0)
  message(FATAL_ERROR "Failed to hipify files!")
endif()
