#pragma once

#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/transport/error.h>

namespace tensorpipe {

// The pipe.
//
// Pipes represent a set of connections between a pair of processes.
// Unlike POSIX pipes, they are message oriented instead of byte
// oriented. Messages that are sent through the pipe may use whatever
// channels are at their disposal to make it happen. If the pair of
// processes happen to be colocated on the same machine, they may
// leverage a region of shared memory to communicate the primary
// buffer of a message. Secondary buffers may use shared memory as
// well, if they're located in CPU memory, or use a CUDA device to
// device copy if they're located in NVIDIA GPU memory. If the pair is
// located across the world, they may simply use a set of TCP
// connections to communicate.
//
class Pipe {
 public:
  using read_descriptor_callback_fn =
      std::function<void(const transport::Error&, const Message&)>;

  void readDescriptor(read_descriptor_callback_fn fn);

  using read_callback_fn =
      std::function<void(const transport::Error&, const Message&)>;

  void read(Message message, read_callback_fn fn);

  using write_callback_fn =
      std::function<void(const transport::Error&, const Message&)>;

  void write(Message message, write_callback_fn fn);
};

} // namespace tensorpipe
