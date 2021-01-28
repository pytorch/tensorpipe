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
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/buffer.h>
#include <tensorpipe/core/buffer_helpers.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/core/nop_types.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/transport/context.h>

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
    channel::TDescriptor descriptor;
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
    SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
    WRITING_PAYLOADS_AND_SENDING_TENSORS,
    FINISHED
  };
  State state{UNINITIALIZED};
  int64_t numPayloadsBeingWritten{0};
  int64_t numTensorDescriptorsBeingCollected{0};
  int64_t numTensorsBeingSent{0};

  // Callbacks.
  Pipe::write_callback_fn writeCallback;

  // Buffers provided by the user.
  Message message;

  // Tensor descriptors collected from the channels.
  struct Tensor {
    DeviceType type;
    std::string channelName;
    channel::TDescriptor descriptor;
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
  OnDemandDeferredExecutor loop_;

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

  template <typename TBuffer>
  using TChannelMap = std::
      unordered_map<std::string, std::shared_ptr<channel::Channel<TBuffer>>>;
  TP_DEVICE_FIELD(TChannelMap<CpuBuffer>, TChannelMap<CudaBuffer>) channels_;

  // The server will set this up when it tell the client to switch to a
  // different connection or to open some channels.
  optional<uint64_t> registrationId_;

  using TChannelRegistrationMap =
      std::unordered_map<std::string, std::vector<uint64_t>>;
  TP_DEVICE_FIELD(TChannelRegistrationMap, TChannelRegistrationMap)
  channelRegistrationIds_;

  using TChannelConnectionsMap = std::unordered_map<
      std::string,
      std::vector<std::shared_ptr<transport::Connection>>>;
  TP_DEVICE_FIELD(TChannelConnectionsMap, TChannelConnectionsMap)
  channelReceivedConnections_;

  ClosingReceiver closingReceiver_;

  std::deque<ReadOperation> readOperations_;
  std::deque<WriteOperation> writeOperations_;

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
  // callbacks. We need to remember the interval of messages for which we're
  // waiting for allocation in order to match calls to read to the right message
  // and for sanity checks. We do so by storing the lower and upper bounds.
  int64_t nextMessageGettingAllocation_{0};
  int64_t nextMessageAskingForAllocation_{0};

  Error error_{Error::kSuccess};

  //
  // Helpers to prepare callbacks from transports and listener
  //

  LazyCallbackWrapper<PipeImpl> lazyCallbackWrapper_{*this, this->loop_};
  EagerCallbackWrapper<PipeImpl> eagerCallbackWrapper_{*this, this->loop_};

  //
  // Helpers to schedule our callbacks into user code
  //

  void callReadDescriptorCallback(ReadOperation& op);
  void callReadCallback(ReadOperation& op);
  void callWriteCallback(WriteOperation& op);

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

  void advanceReadOperation(ReadOperation& op);
  void advanceWriteOperation(WriteOperation& op);

  bool advanceOneReadOperation(ReadOperation& op);
  bool advanceOneWriteOperation(WriteOperation& op);

  void readDescriptorOfMessage(ReadOperation& op);
  void readPayloadsAndReceiveTensorsOfMessage(ReadOperation& op);
  void sendTensorsOfMessage(WriteOperation& op);
  void writeDescriptorAndPayloadsOfMessage(WriteOperation& op);
  void onReadWhileServerWaitingForBrochure(const Packet& nopPacketIn);
  void onReadWhileClientWaitingForBrochureAnswer(const Packet& nopPacketIn);
  void onAcceptWhileServerWaitingForConnection(
      std::string receivedTransport,
      std::shared_ptr<transport::Connection> receivedConnection);
  template <typename TBuffer>
  void onAcceptWhileServerWaitingForChannel(
      std::string channelName,
      size_t connId,
      std::string receivedTransport,
      std::shared_ptr<transport::Connection> receivedConnection);
  void onReadOfMessageDescriptor(ReadOperation& op, const Packet& nopPacketIn);
  void onDescriptorOfTensor(
      WriteOperation& op,
      int64_t tensorIdx,
      channel::TDescriptor descriptor);
  void onReadOfPayload(ReadOperation& op);
  void onRecvOfTensor(ReadOperation& op);
  void onWriteOfPayload(WriteOperation& op);
  void onSendOfTensor(WriteOperation& op);

  ReadOperation* findReadOperation(int64_t sequenceNumber);
  WriteOperation* findWriteOperation(int64_t sequenceNumber);

  template <typename TBuffer>
  const std::map<
      int64_t,
      std::tuple<std::string, std::shared_ptr<channel::Context<TBuffer>>>>&
  getOrderedChannels();

  template <typename TBuffer>
  std::shared_ptr<channel::Context<TBuffer>> getChannelContext(
      const std::string& channelName);

  bool pendingRegistrations();

  template <typename T>
  friend class LazyCallbackWrapper;
  template <typename T>
  friend class EagerCallbackWrapper;
};

} // namespace tensorpipe
