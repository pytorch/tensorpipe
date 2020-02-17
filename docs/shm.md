# The shm transport

This document is an attempt to capture the design principles and inner
working of the shm transport (see `tensorpipe/transport/shm`). Its
performance makes it an efficient alternative to IP based transports
for same-machine communication.

At the core of a transport implementation lies a listener, a
connection, and a context. Listeners accept connections. Contexts
create listeners and can connect to remote listeners.

## Concepts


### Ring buffers

Shared memory ring buffers are a core building block for the shm
transport. They are implemented with split control and data
sections. This means the data section can be fully aligned. The header
section stores a read/write transaction flag and the head and tail
offsets into the data section. Producers and consumers of the ring
buffer use atomic instructions to mutate this header depending on
their intent.

### File descriptors

The header and data segments of a shared memory ring buffer are
created as follows. First, a file is created in `/dev/shm` with the
`O_TMPFILE` flag. This means that anything written to the resulting
file is lost when the last file descriptor is closed, unless the file
is given a name. Because we never give this file a name, the segment
is automatically cleaned up when the last process that has its file
descriptor terminates.

Per above, creating a shared memory ring buffer yields 2 file
descriptors, one for the header segment and one for the data segment.
These file descriptors are shared over a Unix domain socket.

### The reactor

This is a TensorPipe specific component. It uses a shared memory ring
buffer to allow other processes to trigger functions. If a process wants
another process to trigger a function, it registers this function with
the reactor, and gets back a 32-bit token. Then, the file descriptors of
the reactor's ring buffer, as well as the token, are sent to another
process. The other process can now map the reactor ring buffer, and
trigger the registered function by writing the token to the ring buffer.

See [considerations](#considerations) below on why this was used.

### Unix domain sockets

Coordination between process to bootstrap a connection that uses
shared memory ring buffers is implemented using Unix domain sockets.
The listening side of a connection binds and listens on an abstract
socket address. A typical Unix domain socket "address" is a filesystem
pathname. An abstract socket address, by contrast, is not visible on
any filesystem. They exist in a single abstract socket namespace
shared by all processes on the machine. Removing the filesystem
dependency means two things:

1. (+) It is not necessary to purge stale Unix domain socket files.
2. (-) These sockets don't have permissions, so any process that has
   its name can connect.

Read more about abstract domain sockets [here][1] and [here][2].

[1]: http://man7.org/linux/man-pages/man7/unix.7.html
[2]: https://utcc.utoronto.ca/~cks/space/blog/linux/SocketAbstractNamespace

Once processes have established a Unix domain socket, it is used to:

1. Pass the shared memory file descriptors to a peer process.
2. Signal peer termination (through eof on socket closure).
3. ... nothing else. All data moves through the ring buffers.

**Note:** abstract socket addresses are a Linux specific feature.

## Bringing it together

So, to establish one of these shared memory connections, we first
listen on some unique abstract socket address. This address must be
known to the process that wishes to connect. For a quick test we can
use a pre-shared address. Otherwise, we can generate a UUID and share
it with some out of band mechanism. The connecting process connects
and the listening process accepts. We have now established a Unix
domain socket and move on to the next step.

Each process creates a new shared memory ring buffer specifically for
this connection. We refer to this ring buffer as the _inbox_. We
expect each process to be pinned to a specific NUMA node and perform
the memory allocation in the same NUMA domain.

The file descriptors of the inbox, the file descriptors of the
reactor, and a token to trigger readability of the inbox, are shared
over the socket.

Each process receives file descriptors from their peer and initializes
the corresponding ring buffers. The peer's inbox is referred to as the
_outbox_. The token to trigger remote readability is referred to as
the _outbox trigger_.

The connection is now established! Writes are performed by writing
directly into the outbox and triggering the outbox trigger. The
trigger wakes up the peer's reactor and executes a function that
notifies the connection of readability. Subsequently, the connection
checks if there was a pending read operation, and processes it if so.

When either process destructs the connection, or crashes, the original
Unix domain socket is closed, which signals the peer process that it
shouldn't expect more writes to its inbox and can destruct the
connection as well.

## Considerations

A single process may have multiple connections. Therefore, it may have
multiple inbox ring buffers. One way to react to incoming writes is to
simply check if there are any bytes to read. This requires checking all
N inboxes for reads, which can become problematic if N gets large. To
better solve this multiplexing problem we initially used an
[`eventfd(2)`][eventfd] per inbox. This file descriptor was registered
with the existing [`epoll(7)`][epoll] loop and would trigger the
readability function when it became readable. To perform a write, the
peer process would first write to the outbox and then write to the
peer's eventfd.

[eventfd]: http://man7.org/linux/man-pages/man2/eventfd.2.html
[epoll]: http://man7.org/linux/man-pages/man7/epoll.7.html

A simple ping/pong performance benchmark using this approach, with both
processes pinned to the same NUMA node, showed a lower bound latency of
~12 microseconds. This seemed high for a pair of ring buffer writes, so
we explored alternatives, and came up with the reactor approach. Now,
the same benchmark runs with a lower bound latency of about ~1.7
microseconds, which is a 7x improvement over the `eventfd(2)`/`epoll(7)`
approach.
