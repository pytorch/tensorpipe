add_executable(defs_test defs_test.cc)
target_link_libraries(defs_test tensorpipe gtest_main)
add_executable(system_test system_test.cc)
target_link_libraries(system_test tensorpipe gtest_main)
