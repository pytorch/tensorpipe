set(TENSORPIPE_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/util/shm/segment.cc
  )

set(TENSORPIPE_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/common/defs.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/optional.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/message.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/shm/segment.h
  )

add_library(tensorpipe ${TENSORPIPE_SOURCES})
target_include_directories(tensorpipe PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tensorpipe PRIVATE ghc_filesystem)

add_subdirectory(transport/uv)

add_subdirectory(test)
