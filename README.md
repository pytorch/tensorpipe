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

## License

TensorPipe is BSD licensed, as found in the [LICENSE.txt](LICENSE.txt) file.
