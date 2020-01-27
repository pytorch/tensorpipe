add_library(tensorpipe_uv connection.cc error.cc listener.cc loop.cc sockaddr.cc uv.cc)
target_link_libraries(tensorpipe_uv tensorpipe uv)
