add_tensorpipe_test(transport_shm_test
  connection_test.cc
  context_test.cc
  listener_test.cc
  loop_test.cc
  )
target_link_libraries(transport_shm_test tensorpipe_shm)
