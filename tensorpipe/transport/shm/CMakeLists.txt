add_library(tensorpipe_shm fd.cc loop.cc socket.cc)
target_link_libraries(tensorpipe_shm tensorpipe)
