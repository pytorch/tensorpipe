add_library(tensorpipe_shm connection.cc fd.cc listener.cc loop.cc socket.cc)
target_link_libraries(tensorpipe_shm tensorpipe)
