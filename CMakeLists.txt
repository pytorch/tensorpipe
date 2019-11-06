cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

project(tensorpipe C CXX)
set(CMAKE_CXX_STANDARD 14)

# Use CMAKE_<LANG>_COMPILER_LAUNCHER if available.
find_program(SCCACHE_EXECUTABLE sccache)
mark_as_advanced(SCCACHE_EXECUTABLE)
if(SCCACHE_EXECUTABLE)
  foreach(LANG CXX C)
    if(NOT DEFINED CMAKE_${LANG}_COMPILER_LAUNCHER AND NOT CMAKE_${LANG}_COMPILER MATCHES ".*/s?ccache$")
      message(STATUS "Enabling sccache for ${LANG}")
      set(CMAKE_${LANG}_COMPILER_LAUNCHER ${SCCACHE_EXECUTABLE} CACHE STRING "")
    endif()
  endforeach()
endif()

add_subdirectory(tensorpipe)
