add_library(tensorpipe_testing STATIC test.cc)
target_link_libraries(tensorpipe_testing PUBLIC gtest_main)

function(add_tensorpipe_test)
  add_executable(${ARGV})
  target_link_libraries("${ARGV0}"
    -Wl,--whole-archive
    tensorpipe_testing
    -Wl,--no-whole-archive
    tensorpipe)
endfunction()

add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(proto)
add_subdirectory(transport/shm)
add_subdirectory(transport/uv)
add_subdirectory(util/ringbuffer)
add_subdirectory(util/shm)
