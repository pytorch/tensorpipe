set(TENSORPIPE_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.cc
  )

set(TENSORPIPE_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/common/defs.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/optional.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/message.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.h
  )

add_library(tensorpipe ${TENSORPIPE_SOURCES})
target_include_directories(tensorpipe PUBLIC ${CMAKE_SOURCE_DIR})

add_subdirectory(transport/uv)

add_subdirectory(test)
