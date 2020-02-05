/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <mutex>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/proto/all.pb.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {

class Listener;

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
class Pipe final : public std::enable_shared_from_this<Pipe> {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  //
  // Initialization
  //

  static std::shared_ptr<Pipe> create(
      std::shared_ptr<Context>,
      const std::string&);

  Pipe(
      ConstructorToken,
      std::shared_ptr<Context>,
      std::string,
      std::shared_ptr<transport::Connection>);

  Pipe(
      ConstructorToken,
      std::shared_ptr<Context>,
      std::shared_ptr<Listener>,
      std::string,
      std::shared_ptr<transport::Connection>);

  ~Pipe();

  //
  // Entry points for user code
  //

  using read_descriptor_callback_fn =
      std::function<void(const Error&, Message&&)>;

  void readDescriptor(read_descriptor_callback_fn);

  using read_callback_fn = std::function<void(const Error&, Message&&)>;

  void read(Message&&, read_callback_fn);

  using write_callback_fn = std::function<void(const Error&, Message&&)>;

  void write(Message&&, write_callback_fn);

 private:
  enum State {
    INITIALIZING,
    CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE,
    SERVER_WAITING_FOR_BROCHURE,
    CLIENT_WAITING_FOR_BROCHURE_ANSWER,
    SERVER_WAITING_FOR_CONNECTION,
    ESTABLISHED
  };

  State state_{INITIALIZING};

  std::shared_ptr<Context> context_;
  std::shared_ptr<Listener> listener_;

  std::string transport_;
  std::shared_ptr<transport::Connection> connection_;

  // If the server switches to a different connection it stores the old one
  // here so that it is kept open because it's the client that has to close it.
  std::shared_ptr<transport::Connection> initialConnection_;

  // The server will set this up when it tell the client to switch to a
  // different connection.
  optional<uint64_t> registrationId_;

  RearmableCallback<read_descriptor_callback_fn, const Error&, Message>
      readDescriptorCallback_;

  // The descriptors we've notified the user about but weren't accepted yet.
  std::deque<Message> waitingDescriptors_;

  // The reads that were started and that are completing.
  std::deque<std::tuple<Message, read_callback_fn>> pendingReads_;

  std::deque<std::tuple<Message, write_callback_fn>>
      writesWaitingUntilPipeIsEstablished_;

  // The writes for which we've sent the descriptor but haven't been asked for
  // the data yet.
  std::deque<std::tuple<Message, write_callback_fn>> pendingWrites_;

  // The writes for which we've sent the data and are waiting for completion.
  std::deque<std::tuple<Message, write_callback_fn>> completingWrites_;

  Error error_;
  std::mutex mutex_;

  //
  // Initialization
  //

  void start_();

  //
  // Entry points fro callbacks from transports and listener
  // and helpers to prepare them
  //

  using transport_read_packet_callback_fn =
      transport::Connection::read_proto_callback_fn<proto::Packet>;
  using transport_write_callback_fn = transport::Connection::write_callback_fn;
  using accept_callback_fn =
      std::function<void(std::string, std::shared_ptr<transport::Connection>)>;

  using bound_read_packet_callback_fn =
      std::function<void(Pipe&, const proto::Packet&)>;
  using bound_write_callback_fn = std::function<void(Pipe&)>;
  using bound_accept_callback_fn = std::function<
      void(Pipe&, std::string, std::shared_ptr<transport::Connection>)>;

  transport_read_packet_callback_fn wrapReadPacketCallback_(
      bound_read_packet_callback_fn = nullptr);
  void readCallbackEntryPoint_(
      bound_read_packet_callback_fn,
      const transport::Error&,
      const proto::Packet& packet);

  transport_write_callback_fn wrapWriteCallback_(
      bound_write_callback_fn = nullptr);
  void writeCallbackEntryPoint_(
      bound_write_callback_fn,
      const transport::Error&);

  accept_callback_fn wrapAcceptCallback_(bound_accept_callback_fn = nullptr);
  void acceptCallbackEntryPoint_(
      bound_accept_callback_fn,
      std::string,
      std::shared_ptr<transport::Connection>);

  //
  // Helpers to schedule our callbacks into user code
  //

  // The callbacks that are ready to be fired. These are scheduled from
  // anywhere and then retrieved and triggered from the context's caller
  // thread.
  CallbackQueue<read_descriptor_callback_fn, const Error&, Message>
      scheduledReadDescriptorCallbacks_;
  CallbackQueue<read_callback_fn, const Error&, Message>
      scheduledReadCallbacks_;
  CallbackQueue<write_callback_fn, const Error&, Message>
      scheduledWriteCallbacks_;

  void triggerReadDescriptorCallback_(
      read_descriptor_callback_fn&&,
      const Error&,
      Message&&);
  void triggerReadCallback_(read_callback_fn&&, const Error&, Message&&);
  void triggerWriteCallback_(write_callback_fn&&, const Error&, Message&&);

  std::atomic_flag isRunOfScheduledCallbacksTriggered_ = ATOMIC_FLAG_INIT;
  void triggerRunOfScheduledCallbacks_();
  void runScheduledCallbacks_();

  //
  // Error handling
  //

  void flushEverythingOnError_();

  //
  // Everything else
  //

  void doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  void writeWhenEstablished_(Message&&, write_callback_fn);
  void onReadWhileServerWaitingForBrochure_(const proto::Packet&);
  void onReadWhileClientWaitingForBrochureAnswer_(const proto::Packet&);
  void onAcceptWhileServerWaitingForConnection_(
      std::string,
      std::shared_ptr<transport::Connection>);
  void onReadWhenEstablished_(const proto::Packet&);

  friend class Context;
  friend class Listener;
};

} // namespace tensorpipe
