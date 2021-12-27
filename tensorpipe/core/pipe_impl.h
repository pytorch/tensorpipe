/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/buffer.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/state_machine.h>
#include <tensorpipe/core/context_impl.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/core/nop_types.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class ContextImpl;
class ListenerImpl;

struct ReadOperation {
  enum State {
    UNINITIALIZED,
    READING_DESCRIPTOR,
    ASKING_FOR_ALLOCATION,
    ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
    READING_PAYLOADS_AND_RECEIVING_TENSORS,
    FINISHED
  };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneReadingDescriptor{false};
  bool doneGettingAllocation{false};
  uint64_t numPayloadsBeingRead{0};
  uint64_t numTensorsBeingReceived{0};

  // Callbacks.
  Pipe::read_descriptor_callback_fn readDescriptorCallback;
  Pipe::read_callback_fn readCallback;

  // Arguments at creation
  bool hasMissingTargetDevices{false};

  Descriptor descriptor;
  // Buffers allocated by the user.
  Allocation allocation;
};

struct WriteOperation {
  enum State {
    UNINITIALIZED,
    WRITING_PAYLOADS_AND_READING_TARGET_DEVICES,
    WRITING_PAYLOADS_AND_SENDING_TENSORS,
    FINISHED
  };

  // Fields used by the state machine
  uint64_t sequenceNumber{0};
  State state{UNINITIALIZED};

  // Progress flags
  bool doneReadingDescriptorReply{false};
  uint64_t numPayloadsBeingWritten{0};
  uint64_t numTensorsBeingSent{0};

  // Callbacks.
  Pipe::write_callback_fn writeCallback;

  // Arguments at creation
  bool hasMissingTargetDevices{false};

  Message message;

  struct Tensor {
    Device sourceDevice;
    optional<Device> targetDevice;
  };
  std::vector<Tensor> tensors;
};

class PipeImpl final : public std::enable_shared_from_this<PipeImpl> {
 public:
  PipeImpl(
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::string remoteName,
      const std::string& url);

  PipeImpl(
      std::shared_ptr<ContextImpl> context,
      std::shared_ptr<ListenerImpl> listener,
      std::string id,
      std::string remoteName,
      std::string transport,
      std::shared_ptr<transport::Connection> connection);

  // Called by the pipe's constructor.
  void init();

  using read_descriptor_callback_fn = Pipe::read_descriptor_callback_fn;
  using read_callback_fn = Pipe::read_callback_fn;
  using write_callback_fn = Pipe::write_callback_fn;

  void readDescriptor(read_descriptor_callback_fn fn);
  void read(Allocation allocation, read_callback_fn fn);
  void write(Message message, write_callback_fn fn);

  const std::string& getRemoteName();

  void close();

 private:
  void initFromLoop();

  void readDescriptorFromLoop(read_descriptor_callback_fn fn);

  void readFromLoop(Allocation allocation, read_callback_fn fn);

  void writeFromLoop(Message message, write_callback_fn fn);

  void closeFromLoop();

  enum State {
    INITIALIZING,
    CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE,
    SERVER_WAITING_FOR_BROCHURE,
    CLIENT_WAITING_FOR_BROCHURE_ANSWER,
    SERVER_WAITING_FOR_CONNECTIONS,
    ESTABLISHED
  };

  State state_{INITIALIZING};

  std::shared_ptr<ContextImpl> context_;
  std::shared_ptr<ListenerImpl> listener_;

  // An identifier for the pipe, composed of the identifier for the context or
  // listener, combined with an increasing sequence number. It will only be used
  // for logging and debugging purposes.
  std::string id_;

  // The name the user has given to the connect method of the local context (for
  // outgoing pipes) or to the constructor of the context on the remote end (for
  // incoming pipes).
  std::string remoteName_;

  std::string transport_;
  enum ConnectionId { DESCRIPTOR, DESCRIPTOR_REPLY };
  std::shared_ptr<transport::Connection> descriptorConnection_;
  std::shared_ptr<transport::Connection> descriptorReplyConnection_;

  std::unordered_map<std::string, std::shared_ptr<channel::Channel>> channels_;
  std::unordered_map<std::pair<Device, Device>, std::string>
      channelForDevicePair_;

  // The server will set this up when it tell the client to switch to a
  // different connection or to open some channels.
  std::unordered_map<uint64_t, uint64_t> registrationIds_;

  std::unordered_map<std::string, std::vector<uint64_t>>
      channelRegistrationIds_;

  std::unordered_map<
      std::string,
      std::vector<std::shared_ptr<transport::Connection>>>
      channelReceivedConnections_;

  OpsStateMachine<PipeImpl, ReadOperation> readOps_{
      *this,
      &PipeImpl::advanceReadOperation};
  using ReadOpIter = decltype(readOps_)::Iter;
  OpsStateMachine<PipeImpl, WriteOperation> writeOps_{
      *this,
      &PipeImpl::advanceWriteOperation};
  using WriteOpIter = decltype(writeOps_)::Iter;

  // A sequence number for the calls to read and write.
  uint64_t nextMessageBeingRead_{0};
  uint64_t nextMessageBeingWritten_{0};

  // A sequence number for the invocations of the callbacks of read and write.
  uint64_t nextReadDescriptorCallbackToCall_{0};
  uint64_t nextReadCallbackToCall_{0};
  uint64_t nextWriteCallbackToCall_{0};

  // When reading, we first read the descriptor, then signal this to the user,
  // and only once the user has allocated the memory we read the payloads. These
  // members store where we are in this loop, i.e., whether the next buffer we
  // will read from the connection will be a descriptor or a payload, and the
  // sequence number of which message that will be for.
  enum ConnectionState { AWAITING_DESCRIPTOR, AWAITING_PAYLOADS };
  ConnectionState connectionState_{AWAITING_DESCRIPTOR};
  uint64_t messageBeingReadFromConnection_{0};

  // When reading, each message will be presented to the user in order for some
  // memory to be allocated for its payloads and tensors (this happens by
  // calling the readDescriptor callback and waiting for a read call). Under
  // normal operation there will be either 0 or 1 messages whose allocation is
  // pending, but there could be more after an error occurs, as we'll flush all
  // callbacks. We need to remember which is the first such operation for which
  // we're waiting for allocation in order to match calls to read to the right
  // message and for sanity checks. We do so by using a special state in the
  // state machine to identify the next operation that will receive a read call,
  // and store its iterator in this field.
  optional<ReadOpIter> nextMessageGettingAllocation_;

  Error error_{Error::kSuccess};

  //
  // Helpers to prepare callbacks from transports and listener
  //

  CallbackWrapper<PipeImpl> callbackWrapper_{*this, *this->context_};

  //
  // Error handling
  //

  void setError(Error error);

  void handleError();

  //
  // State machines
  //

  // Transitions for the pipe's initial handshake.
  // On the client side:
  void onReadWhileClientWaitingForBrochureAnswer(
      const BrochureAnswer& nopBrochureAnswer);
  // On the server side:
  void onReadWhileServerWaitingForBrochure(const Brochure& nopBrochure);
  void onAcceptWhileServerWaitingForConnection(
      ConnectionId connId,
      std::string receivedTransport,
      std::shared_ptr<transport::Connection> receivedConnection);
  void onAcceptWhileServerWaitingForChannel(
      std::string channelName,
      size_t connId,
      std::string receivedTransport,
      std::shared_ptr<transport::Connection> receivedConnection);

  // State machines for read and write ops.
  void advanceReadOperation(
      ReadOpIter opIter,
      ReadOperation::State prevOpState);
  void advanceWriteOperation(
      WriteOpIter opIter,
      WriteOperation::State prevOpState);

  // Actions (i.e., methods that begin a state transition).
  // For read operations:
  void readDescriptorOfMessage(ReadOpIter opIter);
  void callReadDescriptorCallback(ReadOpIter opIter);
  void expectReadCall(ReadOpIter opIter);
  void readPayloadsOfMessage(ReadOpIter opIter);
  void receiveTensorsOfMessage(ReadOpIter opIter);
  void writeDescriptorReplyOfMessage(ReadOpIter opIter);
  void callReadCallback(ReadOpIter opIter);
  // For write operations:
  void writeDescriptorOfMessage(WriteOpIter opIter);
  void writePayloadsOfMessage(WriteOpIter opIter);
  void readDescriptorReplyOfMessage(WriteOpIter opIter);
  void sendTensorsOfMessage(WriteOpIter opIter);
  void callWriteCallback(WriteOpIter opIter);

  //
  // Everything else
  //

  void initConnection(transport::Connection& connection, uint64_t token);
  uint64_t registerTransport(ConnectionId connId);
  std::vector<uint64_t>& registerChannel(const std::string& channelName);

  bool pendingRegistrations();

  template <typename T>
  friend class CallbackWrapper;

  // Contexts and listeners do sometimes need to call directly into initFromLoop
  // and closeFromLoop, in order to make sure that some of their operations can
  // happen "atomically" on the connection, without possibly other operations
  // occurring in between (e.g., an error).
  friend ContextImpl;
  friend ListenerImpl;
};

} // namespace tensorpipe
