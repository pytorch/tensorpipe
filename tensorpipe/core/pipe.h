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
#include <unordered_map>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/proto/core.pb.h>
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
    SERVER_WAITING_FOR_CONNECTIONS,
    ESTABLISHED
  };

  State state_{INITIALIZING};

  std::shared_ptr<Context> context_;
  std::shared_ptr<Listener> listener_;

  std::string transport_;
  std::shared_ptr<transport::Connection> connection_;
  std::unordered_map<std::string, std::shared_ptr<channel::Channel>> channels_;

  // The server will set this up when it tell the client to switch to a
  // different connection or to open some channels.
  optional<uint64_t> registrationId_;
  std::unordered_map<std::string, uint64_t> channelRegistrationIds_;

  RearmableCallback<read_descriptor_callback_fn, const Error&, Message&&>
      readDescriptorCallback_;

  struct MessageBeingAllocated {
    ssize_t length{-1};
    struct Tensor {
      ssize_t length{-1};
      std::string channelName;
      std::vector<uint8_t> channelDescriptor;
    };
    std::vector<Tensor> tensors;
  };

  struct MessageBeingRead {
    int64_t sequenceNumber{-1};
    Message message;
    std::function<void(const Error&, Message&&)> callback;
    bool dataStillBeingRead{true};
    int64_t numTensorDataStillBeingReceived{0};
  };

  struct MessageBeingWritten {
    int64_t sequenceNumber{-1};
    Message message;
    std::function<void(const Error&, Message&&)> callback;
    bool dataStillBeingWritten{true};
    int64_t numTensorDataStillBeingSent{0};
  };

  std::deque<MessageBeingAllocated> messagesBeingAllocated_;
  int64_t nextMessageBeingRead_{0};
  std::deque<MessageBeingRead> messagesBeingRead_;
  int64_t nextMessageBeingWritten_{0};
  std::deque<MessageBeingWritten> messagesBeingWritten_;

  std::deque<std::tuple<Message, write_callback_fn>>
      writesWaitingUntilPipeIsEstablished_;

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

  using transport_read_callback_fn = transport::Connection::read_callback_fn;
  using bound_read_callback_fn =
      std::function<void(Pipe&, const void*, size_t)>;
  transport_read_callback_fn wrapReadCallback_(
      bound_read_callback_fn = nullptr);
  void readCallbackEntryPoint_(
      bound_read_callback_fn,
      const Error&,
      const void*,
      size_t);

  using transport_read_packet_callback_fn =
      transport::Connection::read_proto_callback_fn<proto::Packet>;
  using bound_read_packet_callback_fn =
      std::function<void(Pipe&, const proto::Packet&)>;
  transport_read_packet_callback_fn wrapReadPacketCallback_(
      bound_read_packet_callback_fn = nullptr);
  void readPacketCallbackEntryPoint_(
      bound_read_packet_callback_fn,
      const Error&,
      const proto::Packet&);

  using transport_write_callback_fn = transport::Connection::write_callback_fn;
  using bound_write_callback_fn = std::function<void(Pipe&)>;
  transport_write_callback_fn wrapWriteCallback_(
      bound_write_callback_fn = nullptr);
  void writeCallbackEntryPoint_(bound_write_callback_fn, const Error&);

  using accept_callback_fn =
      std::function<void(std::string, std::shared_ptr<transport::Connection>)>;
  using bound_accept_callback_fn = std::function<
      void(Pipe&, std::string, std::shared_ptr<transport::Connection>)>;
  accept_callback_fn wrapAcceptCallback_(bound_accept_callback_fn = nullptr);
  void acceptCallbackEntryPoint_(
      bound_accept_callback_fn,
      std::string,
      std::shared_ptr<transport::Connection>);

  using channel_recv_callback_fn = channel::Channel::TRecvCallback;
  using bound_channel_recv_callback_fn = std::function<void(Pipe&)>;
  channel_recv_callback_fn wrapChannelRecvCallback_(
      bound_channel_recv_callback_fn = nullptr);
  void channelRecvCallbackEntryPoint_(bound_channel_recv_callback_fn);

  using channel_send_callback_fn = channel::Channel::TSendCallback;
  using bound_channel_send_callback_fn = std::function<void(Pipe&)>;
  channel_send_callback_fn wrapChannelSendCallback_(
      bound_channel_send_callback_fn = nullptr);
  void channelSendCallbackEntryPoint_(bound_channel_send_callback_fn);

  //
  // Helpers to schedule our callbacks into user code
  //

  void triggerReadDescriptorCallback_(
      read_descriptor_callback_fn&&,
      const Error&,
      Message&&);
  void triggerReadCallback_(read_callback_fn&&, const Error&, Message&&);
  void triggerWriteCallback_(write_callback_fn&&, const Error&, Message&&);

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
  void onAcceptWhileServerWaitingForChannel_(
      std::string,
      std::string,
      std::shared_ptr<transport::Connection>);
  void onReadOfMessageDescriptor_(const proto::Packet&);
  void onReadOfMessageData_(int64_t);
  void onRecvOfTensorData_(int64_t);
  void onWriteOfMessageData_(int64_t);
  void onSendOfTensorData_(int64_t);

  void checkForMessagesDoneReading_();
  void checkForMessagesDoneWriting_();

  friend class Context;
  friend class Listener;
};

} // namespace tensorpipe
