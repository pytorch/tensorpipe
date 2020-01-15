add_library(tensorpipe_uv loop.cc sockaddr.cc)
target_link_libraries(tensorpipe_uv tensorpipe uv)
