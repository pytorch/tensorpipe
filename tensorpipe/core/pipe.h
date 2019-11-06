#pragma once

#include <tensorpipe/core/message.h>
#include <tensorpipe/util/optional.h>

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
  virtual ~Pipe();

  // On the read side, the following happens:
  //
  //   1. Pipe is marked as having a message descriptor.
  //   2. User calls `getMessageDescriptor()`.
  //   3. User populates the message descriptor with valid pointers.
  //   4. User calls `read(Message)`.
  //   5. Pipe is marked as having a message.
  //   6. User calls `getMessage()`.
  //
  virtual tensorpipe::util::optional<Message> getMessageDescriptor() = 0;

  virtual void read(Message message) = 0;

  virtual tensorpipe::util::optional<Message> getMessage() = 0;

  // On the write side, the following happens:
  //
  //   1. User calls `write(Message)` with a valid message.
  //   2. The message is written to the remote side of the pipe.
  //   3. Upon completion, the pipe is marked as having flushed.
  //   4. User calls `reclaimMessage()` to acknowledge.
  //
  virtual void write(Message message) = 0;

  virtual tensorpipe::util::optional<Message> reclaimMessage() = 0;
};

} // namespace tensorpipe
