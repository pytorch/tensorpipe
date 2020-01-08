find_package(Protobuf 3 REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate_cpp(
  PROTOBUF_SOURCES
  PROTOBUF_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/proto/message_descriptor.proto
  )

set(TENSORPIPE_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/error.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/util/shm/segment.cc
  ${PROTOBUF_SOURCES}
  )

set(TENSORPIPE_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/common/defs.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/optional.h
  ${CMAKE_CURRENT_SOURCE_DIR}/common/system.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/listener.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/message.h
  ${CMAKE_CURRENT_SOURCE_DIR}/core/pipe.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/connection.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/context.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/error.h
  ${CMAKE_CURRENT_SOURCE_DIR}/transport/listener.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/consumer.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/producer.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/ringbuffer.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/ringbuffer/shm.h
  ${CMAKE_CURRENT_SOURCE_DIR}/util/shm/segment.h
  ${PROTOBUF_HEADERS}
  )

add_library(tensorpipe ${TENSORPIPE_SOURCES})
target_include_directories(tensorpipe PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tensorpipe PRIVATE ghc_filesystem)
target_link_libraries(tensorpipe PRIVATE ${Protobuf_LIBRARIES})

add_subdirectory(transport/shm)
add_subdirectory(transport/uv)

add_subdirectory(test)
