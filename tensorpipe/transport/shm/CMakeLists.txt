add_library(tensorpipe_shm context.cc connection.cc fd.cc listener.cc loop.cc socket.cc)
target_link_libraries(tensorpipe_shm tensorpipe)
