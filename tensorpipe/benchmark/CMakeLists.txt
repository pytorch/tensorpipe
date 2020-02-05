add_library(benchmark_lib options.cc)

add_executable(benchmark benchmark.cc)
target_link_libraries("benchmark"
  -Wl,--no-whole-archive
  benchmark_lib
  tensorpipe
  tensorpipe_shm
  tensorpipe_uv)
