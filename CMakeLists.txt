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

# Make dependencies available (but don't build by default)
add_subdirectory(third_party/googletest EXCLUDE_FROM_ALL)
add_subdirectory(third_party/libuv EXCLUDE_FROM_ALL)
add_subdirectory(third_party/filesystem EXCLUDE_FROM_ALL)
add_subdirectory(third_party/backward-cpp EXCLUDE_FROM_ALL)

# Add include paths for libuv targets.
# These are defined as relative paths in third_party/libuv/CMakeLists.txt,
# but they don't propagate to downstream targets that depend on libuv.
set_target_properties(uv PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${libuv_SOURCE_DIR}/include"
  )
set_target_properties(uv_a PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${libuv_SOURCE_DIR}/include"
  )

add_subdirectory(tensorpipe)
