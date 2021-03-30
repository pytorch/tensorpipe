/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <tensorpipe/common/cpu_buffer.h>
#if TENSORPIPE_SUPPORTS_CUDA
#include <tensorpipe/common/cuda_buffer.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

namespace tensorpipe {

class ContextImpl;
class ListenerImpl;

struct ReadOperation {
  int64_t sequenceNumber{-1};

  // Progress indicators.
  enum State {
    UNINITIALIZED,
    READING_DESCRIPTOR,
    ASKING_FOR_ALLOCATION,
    ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
    READING_PAYLOADS_AND_RECEIVING_TENSORS,
    FINISHED
  };
  State state{UNINITIALIZED};
  bool doneReadingDescriptor{false};
  bool doneGettingAllocation{false};
  int64_t numPayloadsBeingRead{0};
  int64_t numTensorsBeingReceived{0};

  // Callbacks.
  Pipe::read_descriptor_callback_fn readDescriptorCallback;
  Pipe::read_callback_fn readCallback;

  // Metadata found in the descriptor read from the connection.
  struct Payload {
    ssize_t length{-1};
  };
  std::vector<Payload> payloads;
  struct Tensor {
    DeviceType type;
    ssize_t length{-1};
    std::string channelName;
  };
  std::vector<Tensor> tensors;

  // Buffers allocated by the user.
  Message message;
};

struct WriteOperation {
  int64_t sequenceNumber{-1};

  // Progress indicators.
  enum State {
    UNINITIALIZED,
    SENDING_TENSORS,
    WRITING_PAYLOADS_AND_SENDING_TENSORS,
    FINISHED
  };
  State state{UNINITIALIZED};
  int64_t numPayloadsBeingWritten{0};
  int64_t numTensorsBeingSent{0};

  // Callbacks.
  Pipe::write_callback_fn writeCallback;

  // Buffers provided by the user.
  Message message;

  // Tensor descriptors collected from the channels.
  struct Tensor {
    DeviceType type;
    std::string channelName;
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
  void read(Message message, read_callback_fn fn);
  void write(Message message, write_callback_fn fn);

  const std::string& getRemoteName();

  void close();

 private:
  void initFromLoop();

  void readDescriptorFromLoop(read_descriptor_callback_fn fn);

  void readFromLoop(Message message, read_callback_fn fn);

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
  std::shared_ptr<transport::Connection> connection_;

  std::unordered_map<std::string, std::shared_ptr<channel::Channel>> channels_;

  // The server will set this up when it tell the client to switch to a
  // different connection or to open some channels.
  optional<uint64_t> registrationId_;

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
  int64_t messageBeingReadFromConnection_{0};

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
  // Helpers to schedule our callbacks into user code
  //

  void callReadDescriptorCallback(ReadOpIter opIter);
  void callReadCallback(ReadOpIter opIter);
  void callWriteCallback(WriteOpIter opIter);

  //
  // Error handling
  //

  void setError(Error error);

  void handleError();

  //
  // Everything else
  //

  void startReadingUponEstablishingPipe();
  void startWritingUponEstablishingPipe();

  void advanceReadOperation(
      ReadOpIter opIter,
      ReadOperation::State prevOpState);
  void advanceWriteOperation(
      WriteOpIter opIter,
      WriteOperation::State prevOpState);

  void readDescriptorOfMessage(ReadOpIter opIter);
  void expectReadCall(ReadOpIter opIter);
  void readPayloadsAndReceiveTensorsOfMessage(ReadOpIter opIter);
  void sendTensorsOfMessage(WriteOpIter opIter);
  void writeDescriptorAndPayloadsOfMessage(WriteOpIter opIter);
  void onReadWhileServerWaitingForBrochure(const Packet& nopPacketIn);
  uint64_t registerTransport();
  std::vector<uint64_t>& registerChannel(const std::string& channelName);
  void onReadWhileClientWaitingForBrochureAnswer(const Packet& nopPacketIn);
  void onAcceptWhileServerWaitingForConnection(
      std::string receivedTransport,
      std::shared_ptr<transport::Connection> receivedConnection);
  void onAcceptWhileServerWaitingForChannel(
      std::string channelName,
      size_t connId,
      std::string receivedTransport,
      std::shared_ptr<transport::Connection> receivedConnection);
  void onReadOfMessageDescriptor(ReadOpIter opIter, const Packet& nopPacketIn);
  void onReadOfPayload(ReadOpIter opIter);
  void onRecvOfTensor(ReadOpIter opIter);
  void onWriteOfPayload(WriteOpIter opIter);
  void onSendOfTensor(WriteOpIter opIter);

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
