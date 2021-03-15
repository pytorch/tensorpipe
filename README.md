# TensorPipe

The TensorPipe project provides a tensor-aware channel to transfer rich objects
from one process to another while using the fastest transport for the tensors
contained therein (e.g., CUDA device-to-device copy).

## Getting started

First clone the repository:

```shell
$ git clone --recursive https://github.com/pytorch/tensorpipe
```

Then, build as follows (using ninja instead of make):

``` shell
$ cd tensorpipe
$ mkdir build
$ cd build
$ cmake ../ -GNinja
$ ninja
```

You can find test executables in `build/tensorpipe/test`.

## Interface

There are four classes you need to know about:

- `tensorpipe::Context`, which keeps track of the global state of the system,
  such as thread pools, open file descriptors, etc.
- `tensorpipe::Listener`, which allows one process to open an entry point for
  other processes to connect to.
- `tensorpipe::Pipe`, the one communication primitive that this entire project
  is about. You can obtain one either by connecting to the listener of another
  process or from such a listener when another process connects to it. Once you
  have a pipe, you can send messages on it, and that's the whole point.
- `tensorpipe::Message`, which is the the language that pipes read and write in.
  Pipes are streams of structured messages (not just raw byte buffers), and a
  message is composed of a "core" payload (memory living on CPU) plus a list of
  tensors (memory living on any device, like GPUs).

Sending a message from one end of the pipe to the other can be achieved using
the `write` method, which takes a message (with the data to send) and a
callback which will be invoked once the sending has completed. This callback
will be invoked with an error (if one happened) and with the message.

Receiving a message takes two steps: on an incoming message, first the pipe
asks you to provide some memory to hold the message in, and then you ask the
pipe to read the data into that memory. In order to do this, first you must
register a callback that will be notified for incoming messages. This is
performed by calling the `readDescriptor` method with said callback. The
callback will be invoked with a so-called descriptor, which can be seen as a
"message skeleton", i.e., a message with no buffers attached to it (they are
set to null pointers). The job of this callback is filling in those buffers,
either by allocating the required memory or by obtaining it from somewhere else
(from a cache, as a slice of a batch that's being assembled, ...). This
descriptor also contains some metadata, given by the sender, which can be used
to provide allocation hints or any other information that can help the receiver
determine where to store the data. Once the message's buffers are ready, you
can tell the pipe to go ahead and fill them in with the incoming data by
passing the message to the `read` method, together with a callback which will
be called when all the data has been received and stored. As when writing, this
callback will be given a (possibly empty) error and the original message. The
`readDescriptor` callback is one-shot, which means that after it fires it
"expires" and will not be called again. It must be re-armed for a new event to
be received.

When you pass a message to the pipe, to send it or to receive into it, you must
not tamper with the underlying memory until the callback has completed, even if
the `write` or `read` call already returned. (The `write` and `read` calls, and
all other calls, are non-blocking so that it's easier to schedule asynchronous
parallel trasfers without having to use threads). This means you can not deallocate
the memory or alter it in any way, as the pipe may still be reading or
modifying it. In other terms, you relinquish control over the memory when you
pass a message to the pipe, only to reacquire it once the message is given back
to you in the callback. This contract is encoded by the requirement to move the
messages into and out of the pipe (using rvalue references). Also, because of
this agreement, all callbacks will always be called, even if the pipe is closed
or if it errors, in order to give back the memory.

The order in which messages are written to a pipe is preserved when these
messages are read on the other side. Moreover, for a given pipe endpoint, the
callbacks of the performed operations are executed in the same order that these
operations were scheduled, even if the operations are performed asynchronously
or out-of-band and thus may overlap or occur out of order. What this means is
that if two write operations are scheduled one after the other back-to-back,
even if the second one completes before the first one, its callback is delayed
until the first one also completes and its callback is invoked. The same
applies for reads. All the callbacks of all the pipes in a given context are
called from the same per-context thread and thus no two callbacks will occur at
the same time. However, different contexts will use different threads and their
callbacks may thus overlap.

All the callbacks are invoked with an error reference. This may be "empty",
i.e., indicate that no error has in fact occurred. In this case, the error
object evaluates to false. In case of an actual error it will instead evaluate
to true. When invoked with an error, the remaining arguments of the callback
may be meaningless. For the `read` and `write` callbacks they will still
contain the message that these methods will be invoked with, but the
`readDescriptor` one will be an empty or invalid message. It should not be
used.

There is no expectation for the `readDescriptor` callback to be armed at all
times. Similarly, it is not necessary to call the `read` method immediately
after a descriptor has been read. Both these possibilities are by design, in
order to allow the user of the pipe to apply some backpressure in case it's
receiving messages at a faster rate than it can handle, or for any other
reason. This backpressure will be propagated to the lower-level components as
as far down as possible (e.g., by stopping listening for readability events on
the socket file descriptor).

## Transports and channels

TensorPipe aims to be "backend-agnostic": it doesn't want to be restricted to a
single way of copying data around but wants to be able to choose the fastest
medium from a library of backends, based on the circumstances (e.g., are the two
processes on the same machine?) and on the available hardware (e.g., are the
GPUs connected with NVLink?). TensorPipe strives to have the largest selection
of backends, enabling users to implement specific backends for their systems
(should the default ones prove limited) and encouraging contributions.

The two processes that are establishing a pipe will automatically negotiate
during setup to determine which of the backends they have at their disposal can
be used and how well they would perform, in order to choose the best one in a
way that is completely transparent to the user.

Backends come in two flavors:

- Transports are the connections used by the pipes to transfer control messages,
  and the (smallish) core payloads. They are meant to be lightweight and
  low-latency. The most basic transport is a simple TCP one, which should work
  in all scenarios. A more optimized one, for example, is based on a ring buffer
  allocated in shared memory, which two processes on the same machine can use to
  communicate by performing just a memory copy, without passing through the
  kernel.

- Channels are where the heavy lifting takes place, as they take care of copying
  the (larger) tensor data. High bandwidths are a requirement. Examples include
  multiplexing chunks of data across multiple TCP sockets and processes, so to
  saturate the NIC's bandwidth. Or using a CUDA memcpy call to transfer memory
  from one GPU to another using NVLink.

These different usage patterns promote different design choices when
implementing transports and channels, which means the two are not perfectly
interchangeable. For example, a TCP-based transport is best implemented using a
single connection, whereas a TCP-based channel will benefit from using multiple
connection and chunk and multiplex the payload over them in order to saturate
the bandwidth even on the most powerful NICs.

Moreover, the APIs of transports and channels put different constraints on
them, which demand and permit different approaches. As a rule of thumb, we
require more from the transports: the only out-of-band information they can use
is a simple address, which is all they can use to bootstrap the connection, and
they need to include some "signaling" capabilities (a write on one side "wakes
up" the other side by causing a read). Channels, on the other hand, have much
looser requirements: they basically just need to implement a `memcpy` and, for
anything beyond that, they can leverage a transport that the pipe gives to them
for support.

## License

TensorPipe is BSD licensed, as found in the [LICENSE.txt](LICENSE.txt) file.
