add_library(tensorpipe_testing STATIC test.cc)
target_link_libraries(tensorpipe_testing PUBLIC gtest_main)

function(add_tensorpipe_test)
  add_executable(${ARGV})
  target_link_libraries("${ARGV0}"
    -Wl,--whole-archive
    tensorpipe_testing
    -Wl,--no-whole-archive
    tensorpipe
    tensorpipe_shm)
endfunction()

add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(proto)
add_subdirectory(transport/shm)
add_subdirectory(util/ringbuffer)
add_subdirectory(util/shm)

add_executable(tensorpipe_test
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/uv/loop_test.cc
  )
target_link_libraries(tensorpipe_test tensorpipe_shm)
target_link_libraries(tensorpipe_test tensorpipe_uv)
target_link_libraries(tensorpipe_test gtest_main)

enable_testing()
add_test(tensorpipe_test tensorpipe_test)
