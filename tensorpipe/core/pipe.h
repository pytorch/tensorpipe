/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>

#include <tensorpipe/common/error.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/transport/connection.h>

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
class Pipe final {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  //
  // Initialization
  //

  Pipe(
      ConstructorToken,
      std::shared_ptr<Context::PrivateIface> context,
      std::string id,
      std::string remoteName,
      const std::string& url);

  Pipe(
      ConstructorToken,
      std::shared_ptr<Context::PrivateIface> context,
      std::shared_ptr<Listener::PrivateIface> listener,
      std::string id,
      std::string remoteName,
      std::string transport,
      std::shared_ptr<transport::Connection> connection);

  //
  // Entry points for user code
  //

  using read_descriptor_callback_fn =
      std::function<void(const Error&, Message)>;

  void readDescriptor(read_descriptor_callback_fn);

  using read_callback_fn = std::function<void(const Error&, Message)>;

  void read(Message, read_callback_fn);

  using write_callback_fn = std::function<void(const Error&, Message)>;

  void write(Message, write_callback_fn);

  // Retrieve the user-defined name that was given to the constructor of the
  // context on the remote side, if any (if not, this will be the empty string).
  // This is intended to help in logging and debugging only.
  const std::string& getRemoteName();

  // Put the pipe in a terminal state, aborting its pending operations and
  // rejecting future ones, and release its resrouces. This may be carried out
  // asynchronously, in background.
  void close();

  ~Pipe();

 private:
  class Impl;

  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  std::shared_ptr<Impl> impl_;

  // Allow context to access constructor token.
  friend class Context;
  // Allow listener to access constructor token.
  friend class Listener;
};

} // namespace tensorpipe
