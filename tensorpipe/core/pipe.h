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
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
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
      std::function<void(const Error&, Message)>;

  void readDescriptor(read_descriptor_callback_fn);

  using read_callback_fn = std::function<void(const Error&, Message)>;

  void read(Message, read_callback_fn);

  using write_callback_fn = std::function<void(const Error&, Message)>;

  void write(Message, write_callback_fn);

 private:
  std::mutex mutex_;
  std::atomic<std::thread::id> currentLoop_{std::thread::id()};
  std::deque<std::function<void()>> pendingTasks_;

  void deferToLoop_(std::function<void()> fn);

  void startFromLoop_();

  void readDescriptorFromLoop_(read_descriptor_callback_fn);

  void readFromLoop_(Message, read_callback_fn);

  void writeFromLoop_(Message, write_callback_fn);

  bool inLoop_();

  void closeFromLoop_();

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

  enum ConnectionState { NEXT_UP_IS_DESCRIPTOR, NEXT_UP_IS_DATA };

  ConnectionState connectionState_{NEXT_UP_IS_DESCRIPTOR};

  struct MessageBeingExpected {
    int64_t sequenceNumber{-1};
    read_descriptor_callback_fn callback;
  };

  struct MessageBeingAllocated {
    int64_t sequenceNumber{-1};
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
    read_callback_fn callback;
    bool dataStillBeingRead{true};
    int64_t numTensorDataStillBeingReceived{0};
  };

  struct MessageBeingQueued {
    int64_t sequenceNumber{-1};
    Message message;
    write_callback_fn callback;
  };

  struct MessageBeingWritten {
    int64_t sequenceNumber{-1};
    Message message;
    write_callback_fn callback;
    bool dataStillBeingWritten{true};
    int64_t numTensorDataStillBeingSent{0};
  };

  int64_t nextMessageBeingRead_{0};
  std::deque<MessageBeingExpected> messagesBeingExpected_;
  int64_t nextReadDescriptorCallbackToCall_{0};
  std::deque<MessageBeingAllocated> messagesBeingAllocated_;
  std::deque<MessageBeingRead> messagesBeingRead_;
  int64_t nextReadCallbackToCall_{0};

  int64_t nextMessageBeingWritten_{0};
  std::deque<MessageBeingQueued> messagesBeingQueued_;
  std::deque<MessageBeingWritten> messagesBeingWritten_;
  int64_t nextWriteCallbackToCall_{0};

  Error error_;

  //
  // Initialization
  //

  void start_();

  //
  // Helpers to prepare callbacks from transports and listener
  //

  DeferringTolerantCallbackWrapper<Pipe, const void*, size_t>
      readCallbackWrapper_;
  DeferringCallbackWrapper<Pipe> readPacketCallbackWrapper_;
  DeferringTolerantCallbackWrapper<Pipe> writeCallbackWrapper_;
  DeferringCallbackWrapper<Pipe> writePacketCallbackWrapper_;
  DeferringCallbackWrapper<
      Pipe,
      std::string,
      std::shared_ptr<transport::Connection>>
      connectionRequestCallbackWrapper_;
  DeferringTolerantCallbackWrapper<Pipe> channelRecvCallbackWrapper_;
  DeferringTolerantCallbackWrapper<Pipe> channelSendCallbackWrapper_;

  //
  // Helpers to schedule our callbacks into user code
  //

  void triggerReadDescriptorCallback_(
      int64_t,
      read_descriptor_callback_fn&&,
      const Error&,
      Message);
  void triggerReadCallback_(int64_t, read_callback_fn&&, const Error&, Message);
  void triggerWriteCallback_(
      int64_t,
      write_callback_fn&&,
      const Error&,
      Message);

  //
  // Error handling
  //

  void handleError_();

  //
  // Everything else
  //

  void doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  void writeWhenEstablished_(int64_t, Message, write_callback_fn);
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
  template <typename T, typename... Args>
  friend class DeferringCallbackWrapper;
  template <typename T, typename... Args>
  friend class DeferringTolerantCallbackWrapper;
};

} // namespace tensorpipe
