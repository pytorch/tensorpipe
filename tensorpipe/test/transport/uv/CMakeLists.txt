add_tensorpipe_test(transport_uv_test
  listener_test.cc
  loop_test.cc
  sockaddr_test.cc
  )
target_link_libraries(transport_uv_test tensorpipe_uv)
