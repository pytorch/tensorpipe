/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/pipe.h>

#include <algorithm>
#include <deque>
#include <mutex>
#include <unordered_map>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/proto/core.pb.h>

namespace tensorpipe {

namespace {

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
    ssize_t length{-1};
    std::string channelName;
    channel::Channel::TDescriptor descriptor;
  };
  std::vector<Tensor> tensors;

  // Buffers allocated by the user.
  Message message;
};

// Copy the payload and tensors sizes, the tensor descriptors, etc. from the
// message descriptor that is contained in the protobuf to the ReadOperation.
void parseDescriptorOfMessage(
    ReadOperation& op,
    const proto::Packet& pbPacketIn) {
  Message& message = op.message;

  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kMessageDescriptor);
  const proto::MessageDescriptor& pbMessageDescriptor =
      pbPacketIn.message_descriptor();

  message.metadata = pbMessageDescriptor.metadata();
  for (const auto& pbPayloadDescriptor :
       pbMessageDescriptor.payload_descriptors()) {
    Message::Payload payload;
    ReadOperation::Payload payloadBeingAllocated;
    payload.length = pbPayloadDescriptor.size_in_bytes();
    payloadBeingAllocated.length = payload.length;
    payload.metadata = pbPayloadDescriptor.metadata();
    message.payloads.push_back(std::move(payload));
    op.payloads.push_back(std::move(payloadBeingAllocated));
  }
  for (const auto& pbTensorDescriptor :
       pbMessageDescriptor.tensor_descriptors()) {
    Message::Tensor tensor;
    ReadOperation::Tensor tensorBeingAllocated;
    tensor.length = pbTensorDescriptor.size_in_bytes();
    tensorBeingAllocated.length = tensor.length;
    tensor.metadata = pbTensorDescriptor.metadata();
    tensorBeingAllocated.channelName = pbTensorDescriptor.channel_name();
    // FIXME If the protobuf wasn't const we could move the string out...
    tensorBeingAllocated.descriptor = pbTensorDescriptor.channel_descriptor();
    message.tensors.push_back(std::move(tensor));
    op.tensors.push_back(std::move(tensorBeingAllocated));
  }
}

// Raise an error if the number or sizes of the payloads and the tensors in the
// message do not match the ones that are expected by the ReadOperation.
void checkAllocationCompatibility(
    const ReadOperation& op,
    const Message& message) {
  size_t numPayloads = message.payloads.size();
  TP_THROW_ASSERT_IF(numPayloads != op.payloads.size());
  for (size_t payloadIdx = 0; payloadIdx < numPayloads; payloadIdx++) {
    const Message::Payload& payload = message.payloads[payloadIdx];
    const ReadOperation::Payload& payloadBeingAllocated =
        op.payloads[payloadIdx];
    TP_DCHECK_GE(payloadBeingAllocated.length, 0);
    TP_THROW_ASSERT_IF(payload.length != payloadBeingAllocated.length);
  }
  size_t numTensors = message.tensors.size();
  TP_THROW_ASSERT_IF(numTensors != op.tensors.size());
  for (size_t tensorIdx = 0; tensorIdx < numTensors; tensorIdx++) {
    const Message::Tensor& tensor = message.tensors[tensorIdx];
    const ReadOperation::Tensor& tensorBeingAllocated = op.tensors[tensorIdx];
    TP_DCHECK_GE(tensorBeingAllocated.length, 0);
    TP_THROW_ASSERT_IF(tensor.length != tensorBeingAllocated.length);
  }
}

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
    std::string channelName;
    channel::Channel::TDescriptor descriptor;
  };
  std::vector<Tensor> tensors;
};

// Produce a protobuf containing a message descriptor using the information
// contained in the WriteOperation: number and sizes of payloads and tensors,
// tensor descriptors, ...
std::shared_ptr<proto::Packet> makeDescriptorForMessage(
    const WriteOperation& op) {
  auto pbPacketOut = std::make_shared<proto::Packet>();
  proto::MessageDescriptor* pbMessageDescriptor =
      pbPacketOut->mutable_message_descriptor();

  pbMessageDescriptor->set_metadata(op.message.metadata);

  for (int payloadIdx = 0; payloadIdx < op.message.payloads.size();
       ++payloadIdx) {
    const Message::Payload& payload = op.message.payloads[payloadIdx];
    proto::MessageDescriptor::PayloadDescriptor* pbPayloadDescriptor =
        pbMessageDescriptor->add_payload_descriptors();
    pbPayloadDescriptor->set_size_in_bytes(payload.length);
    pbPayloadDescriptor->set_metadata(payload.metadata);
  }

  TP_DCHECK_EQ(op.message.tensors.size(), op.tensors.size());
  for (int tensorIdx = 0; tensorIdx < op.tensors.size(); ++tensorIdx) {
    const Message::Tensor& tensor = op.message.tensors[tensorIdx];
    const WriteOperation::Tensor& otherTensor = op.tensors[tensorIdx];
    proto::MessageDescriptor::TensorDescriptor* pbTensorDescriptor =
        pbMessageDescriptor->add_tensor_descriptors();
    pbTensorDescriptor->set_device_type(proto::DeviceType::DEVICE_TYPE_CPU);
    pbTensorDescriptor->set_size_in_bytes(tensor.length);
    pbTensorDescriptor->set_metadata(tensor.metadata);
    pbTensorDescriptor->set_channel_name(otherTensor.channelName);
    // FIXME In principle we could move here.
    pbTensorDescriptor->set_channel_descriptor(otherTensor.descriptor);
  }

  return pbPacketOut;
}

} // namespace

class Pipe::Impl : public std::enable_shared_from_this<Pipe::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::string id,
      std::string remoteName,
      const std::string& url);

  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::shared_ptr<Listener::PrivateIface> listener,
      std::string id,
      std::string remoteName,
      std::string transport,
      std::shared_ptr<transport::Connection> connection);

  // Called by the pipe's constructor.
  void init();

  void readDescriptor(read_descriptor_callback_fn);
  void read(Message, read_callback_fn);
  void write(Message, write_callback_fn);

  const std::string& getRemoteName();

  void close();

 private:
  OnDemandLoop loop_;

  void initFromLoop_();

  void readDescriptorFromLoop_(read_descriptor_callback_fn);

  void readFromLoop_(Message, read_callback_fn);

  void writeFromLoop_(Message, write_callback_fn);

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

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<Listener::PrivateIface> listener_;

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
  std::unordered_map<std::string, uint64_t> channelRegistrationIds_;

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

  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, this->loop_};
  EagerCallbackWrapper<Impl> eagerCallbackWrapper_{*this, this->loop_};

  //
  // Helpers to schedule our callbacks into user code
  //

  void callReadDescriptorCallback_(ReadOperation& op);
  void callReadCallback_(ReadOperation& op);
  void callWriteCallback_(WriteOperation& op);

  //
  // Error handling
  //

  void setError_(Error error);

  void handleError_();

  //
  // Everything else
  //

  void startReadingUponEstablishingPipe_();
  void startWritingUponEstablishingPipe_();

  void advanceReadOperation_(ReadOperation& op);
  void advanceWriteOperation_(WriteOperation& op);

  bool advanceOneReadOperation_(ReadOperation& op);
  bool advanceOneWriteOperation_(WriteOperation& op);

  void readDescriptorOfMessage_(ReadOperation&);
  void readPayloadsAndReceiveTensorsOfMessage(ReadOperation&);
  void sendTensorsOfMessage_(WriteOperation&);
  void writeDescriptorAndPayloadsOfMessage_(WriteOperation&);
  void onReadWhileServerWaitingForBrochure_(const proto::Packet&);
  void onReadWhileClientWaitingForBrochureAnswer_(const proto::Packet&);
  void onAcceptWhileServerWaitingForConnection_(
      std::string,
      std::shared_ptr<transport::Connection>);
  void onAcceptWhileServerWaitingForChannel_(
      std::string,
      std::string,
      std::shared_ptr<transport::Connection>);
  void onReadOfMessageDescriptor_(ReadOperation&, const proto::Packet&);
  void onDescriptorOfTensor_(
      WriteOperation&,
      int64_t,
      channel::Channel::TDescriptor);
  void onReadOfPayload_(ReadOperation&);
  void onRecvOfTensor_(ReadOperation&);
  void onWriteOfPayload_(WriteOperation&);
  void onSendOfTensor_(WriteOperation&);

  ReadOperation* findReadOperation(int64_t sequenceNumber);
  WriteOperation* findWriteOperation(int64_t sequenceNumber);

  template <typename T>
  friend class LazyCallbackWrapper;
  template <typename T>
  friend class EagerCallbackWrapper;
};

//
// Initialization
//

Pipe::Pipe(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::string id,
    std::string remoteName,
    const std::string& url)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(id),
          std::move(remoteName),
          url)) {
  impl_->init();
}

Pipe::Pipe(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<Listener::PrivateIface> listener,
    std::string id,
    std::string remoteName,
    std::string transport,
    std::shared_ptr<transport::Connection> connection)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(listener),
          std::move(id),
          std::move(remoteName),
          std::move(transport),
          std::move(connection))) {
  impl_->init();
}

Pipe::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::string id,
    std::string remoteName,
    const std::string& url)
    : state_(CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE),
      context_(std::move(context)),
      id_(std::move(id)),
      remoteName_(std::move(remoteName)),
      closingReceiver_(context_, context_->getClosingEmitter()) {
  std::string address;
  std::tie(transport_, address) = splitSchemeOfURL(url);
  connection_ = context_->getTransport(transport_)->connect(std::move(address));
  connection_->setId(id_ + ".tr_" + transport_);
}

Pipe::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<Listener::PrivateIface> listener,
    std::string id,
    std::string remoteName,
    std::string transport,
    std::shared_ptr<transport::Connection> connection)
    : state_(SERVER_WAITING_FOR_BROCHURE),
      context_(std::move(context)),
      listener_(std::move(listener)),
      id_(std::move(id)),
      remoteName_(std::move(remoteName)),
      transport_(std::move(transport)),
      connection_(std::move(connection)),
      closingReceiver_(context_, context_->getClosingEmitter()) {
  connection_->setId(id_ + ".tr_" + transport_);
}

void Pipe::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Pipe::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
  if (state_ == CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE) {
    auto pbPacketOut = std::make_shared<proto::Packet>();
    // This makes the packet contain a SpontaneousConnection message.
    proto::SpontaneousConnection* pbSpontaneousConnection =
        pbPacketOut->mutable_spontaneous_connection();
    pbSpontaneousConnection->set_context_name(context_->getName());
    TP_VLOG(3) << "Pipe " << id_
               << " is writing proto (spontaneous connection)";
    connection_->write(
        *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing proto (spontaneous connection)";
        }));

    auto pbPacketOut2 = std::make_shared<proto::Packet>();
    proto::Brochure* pbBrochure = pbPacketOut2->mutable_brochure();
    auto pbAllTransportAdvertisements =
        pbBrochure->mutable_transport_advertisement();
    for (const auto& transportContextIter : context_->getOrderedTransports()) {
      const std::string& transportName =
          std::get<0>(transportContextIter.second);
      const transport::Context& transportContext =
          *(std::get<1>(transportContextIter.second));
      proto::TransportAdvertisement* pbTransportAdvertisement =
          &(*pbAllTransportAdvertisements)[transportName];
      pbTransportAdvertisement->set_domain_descriptor(
          transportContext.domainDescriptor());
    }
    auto pbAllChannelAdvertisements =
        pbBrochure->mutable_channel_advertisement();
    for (const auto& channelContextIter : context_->getOrderedChannels()) {
      const std::string& channelName = std::get<0>(channelContextIter.second);
      const channel::Context& channelContext =
          *(std::get<1>(channelContextIter.second));
      proto::ChannelAdvertisement* pbChannelAdvertisement =
          &(*pbAllChannelAdvertisements)[channelName];
      pbChannelAdvertisement->set_domain_descriptor(
          channelContext.domainDescriptor());
    }
    TP_VLOG(3) << "Pipe " << id_ << " is writing proto (brochure)";
    connection_->write(
        *pbPacketOut2, lazyCallbackWrapper_([pbPacketOut2](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done writing proto (brochure)";
        }));
    state_ = CLIENT_WAITING_FOR_BROCHURE_ANSWER;
    auto pbPacketIn = std::make_shared<proto::Packet>();
    TP_VLOG(3) << "Pipe " << id_ << " is reading proto (brochure answer)";
    connection_->read(
        *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done reading proto (brochure answer)";
          impl.onReadWhileClientWaitingForBrochureAnswer_(*pbPacketIn);
        }));
  }
  if (state_ == SERVER_WAITING_FOR_BROCHURE) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    TP_VLOG(3) << "Pipe " << id_ << " is reading proto (brochure)";
    connection_->read(
        *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done reading proto (brochure)";
          impl.onReadWhileServerWaitingForBrochure_(*pbPacketIn);
        }));
  }
}

const std::string& Pipe::getRemoteName() {
  return impl_->getRemoteName();
}

const std::string& Pipe::Impl::getRemoteName() {
  return remoteName_;
}

Pipe::~Pipe() {
  close();
}

void Pipe::close() {
  impl_->close();
}

void Pipe::Impl::close() {
  loop_.deferToLoop([this]() { closeFromLoop_(); });
}

void Pipe::Impl::closeFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(1) << "Pipe " << id_ << " is closing";
  setError_(TP_CREATE_ERROR(PipeClosedError));
}

//
// Entry points for user code
//

void Pipe::readDescriptor(read_descriptor_callback_fn fn) {
  impl_->readDescriptor(std::move(fn));
}

void Pipe::Impl::readDescriptor(read_descriptor_callback_fn fn) {
  loop_.deferToLoop([this, fn{std::move(fn)}]() mutable {
    readDescriptorFromLoop_(std::move(fn));
  });
}

void Pipe::Impl::readDescriptorFromLoop_(read_descriptor_callback_fn fn) {
  TP_DCHECK(loop_.inLoop());

  readOperations_.emplace_back();
  ReadOperation& op = readOperations_.back();
  op.sequenceNumber = nextMessageBeingRead_++;

  TP_VLOG(1) << "Pipe " << id_ << " received a readDescriptor request (#"
             << op.sequenceNumber << ")";

  fn = [this, sequenceNumber{op.sequenceNumber}, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextReadDescriptorCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a readDescriptor callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG(1) << "Pipe " << id_ << " done calling a readDescriptor callback (#"
               << sequenceNumber << ")";
  };

  op.readDescriptorCallback = std::move(fn);

  advanceReadOperation_(op);
}

void Pipe::read(Message message, read_callback_fn fn) {
  impl_->read(std::move(message), std::move(fn));
}

void Pipe::Impl::read(Message message, read_callback_fn fn) {
  // Messages aren't copyable and thus if a lambda captures them it cannot be
  // wrapped in a std::function. Therefore we wrap Messages in shared_ptrs.
  auto sharedMessage = std::make_shared<Message>(std::move(message));
  loop_.deferToLoop([this,
                     sharedMessage{std::move(sharedMessage)},
                     fn{std::move(fn)}]() mutable {
    readFromLoop_(std::move(*sharedMessage), std::move(fn));
  });
}

void Pipe::Impl::readFromLoop_(Message message, read_callback_fn fn) {
  TP_DCHECK(loop_.inLoop());

  // This is such a bad logical error on the user's side that it doesn't deserve
  // to pass through the channel for "expected errors" (i.e., the callback).
  // This check fails when there is no message for which we are expecting an
  // allocation.
  TP_THROW_ASSERT_IF(
      nextMessageGettingAllocation_ == nextMessageAskingForAllocation_);

  ReadOperation* opPtr = findReadOperation(nextMessageGettingAllocation_);
  TP_DCHECK(opPtr != nullptr);
  ++nextMessageGettingAllocation_;
  ReadOperation& op = *opPtr;

  checkAllocationCompatibility(op, message);

  fn = [this, sequenceNumber{op.sequenceNumber}, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextReadCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a read callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG(1) << "Pipe " << id_ << " done calling a read callback (#"
               << sequenceNumber << ")";
  };

  TP_DCHECK_EQ(op.state, ReadOperation::ASKING_FOR_ALLOCATION);
  op.message = std::move(message);
  op.readCallback = std::move(fn);
  op.doneGettingAllocation = true;

  TP_VLOG(1) << "Pipe " << id_ << " received a read request (#"
             << op.sequenceNumber << ", containing "
             << op.message.payloads.size() << " payloads and "
             << op.message.tensors.size() << " tensors)";

  advanceReadOperation_(op);
}

void Pipe::Impl::readPayloadsAndReceiveTensorsOfMessage(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  TP_DCHECK_EQ(op.state, ReadOperation::ASKING_FOR_ALLOCATION);
  op.state = ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS;

  TP_VLOG(2) << "Pipe " << id_
             << " is reading payloads and receiving tensors of message #"
             << op.sequenceNumber;

  TP_DCHECK_EQ(connectionState_, AWAITING_PAYLOADS);
  TP_DCHECK_EQ(messageBeingReadFromConnection_, op.sequenceNumber);
  for (size_t payloadIdx = 0; payloadIdx < op.message.payloads.size();
       payloadIdx++) {
    Message::Payload& payload = op.message.payloads[payloadIdx];
    TP_VLOG(3) << "Pipe " << id_ << " is reading payload #" << op.sequenceNumber
               << "." << payloadIdx;
    connection_->read(
        payload.data,
        payload.length,
        eagerCallbackWrapper_(
            [&op, payloadIdx](
                Impl& impl, const void* /* unused */, size_t /* unused */) {
              TP_VLOG(3) << "Pipe " << impl.id_ << " done reading payload #"
                         << op.sequenceNumber << "." << payloadIdx;
              impl.onReadOfPayload_(op);
            }));
    ++op.numPayloadsBeingRead;
  }
  connectionState_ = AWAITING_DESCRIPTOR;
  ++messageBeingReadFromConnection_;

  for (size_t tensorIdx = 0; tensorIdx < op.message.tensors.size();
       tensorIdx++) {
    Message::Tensor& tensor = op.message.tensors[tensorIdx];
    ReadOperation::Tensor& tensorBeingAllocated = op.tensors[tensorIdx];
    std::shared_ptr<channel::Channel> channel =
        channels_.at(tensorBeingAllocated.channelName);
    TP_VLOG(3) << "Pipe " << id_ << " is receiving tensor #"
               << op.sequenceNumber << "." << tensorIdx;
    channel->recv(
        std::move(tensorBeingAllocated.descriptor),
        tensor.data,
        tensor.length,
        eagerCallbackWrapper_([&op, tensorIdx](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done receiving tensor #"
                     << op.sequenceNumber << "." << tensorIdx;
          impl.onRecvOfTensor_(op);
        }));
    ++op.numTensorsBeingReceived;
  }
}

void Pipe::write(Message message, write_callback_fn fn) {
  impl_->write(std::move(message), std::move(fn));
}

void Pipe::Impl::write(Message message, write_callback_fn fn) {
  // Messages aren't copyable and thus if a lambda captures them it cannot be
  // wrapped in a std::function. Therefore we wrap Messages in shared_ptrs.
  auto sharedMessage = std::make_shared<Message>(std::move(message));
  loop_.deferToLoop([this,
                     sharedMessage{std::move(sharedMessage)},
                     fn{std::move(fn)}]() mutable {
    writeFromLoop_(std::move(*sharedMessage), std::move(fn));
  });
}

void Pipe::Impl::writeFromLoop_(Message message, write_callback_fn fn) {
  TP_DCHECK(loop_.inLoop());

  writeOperations_.emplace_back();
  WriteOperation& op = writeOperations_.back();
  op.sequenceNumber = nextMessageBeingWritten_++;

  TP_VLOG(1) << "Pipe " << id_ << " received a write request (#"
             << op.sequenceNumber << ", contaning " << message.payloads.size()
             << " payloads and " << message.tensors.size() << " tensors)";

  fn = [this, sequenceNumber{op.sequenceNumber}, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextWriteCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a write callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG(1) << "Pipe " << id_ << " done calling a write callback (#"
               << sequenceNumber << ")";
  };

  op.message = std::move(message);
  op.writeCallback = std::move(fn);

  advanceWriteOperation_(op);
}

//
// Helpers to schedule our callbacks into user code
//

void Pipe::Impl::callReadDescriptorCallback_(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  TP_DCHECK(
      op.state == ReadOperation::UNINITIALIZED ||
      op.state == ReadOperation::READING_DESCRIPTOR);
  op.state = ReadOperation::ASKING_FOR_ALLOCATION;

  TP_DCHECK_EQ(op.sequenceNumber, nextMessageAskingForAllocation_);
  ++nextMessageAskingForAllocation_;

  op.readDescriptorCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.readDescriptorCallback = nullptr;
}

void Pipe::Impl::callReadCallback_(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  TP_DCHECK(
      op.state == ReadOperation::ASKING_FOR_ALLOCATION ||
      op.state == ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS);
  op.state = ReadOperation::FINISHED;

  op.readCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.readCallback = nullptr;
}

void Pipe::Impl::callWriteCallback_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  TP_DCHECK(
      op.state == WriteOperation::UNINITIALIZED ||
      op.state == WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS ||
      op.state == WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS);
  op.state = WriteOperation::FINISHED;

  op.writeCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.writeCallback = nullptr;
}

//
// Error handling
//

void Pipe::Impl::setError_(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError_();
}

void Pipe::Impl::handleError_() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(2) << "Pipe " << id_ << " is handling error " << error_.what();

  connection_->close();
  for (auto& channelIter : channels_) {
    channelIter.second->close();
  }

  if (registrationId_.has_value()) {
    listener_->unregisterConnectionRequest(registrationId_.value());
    registrationId_.reset();
  }
  for (const auto& iter : channelRegistrationIds_) {
    listener_->unregisterConnectionRequest(iter.second);
  }
  channelRegistrationIds_.clear();

  if (!readOperations_.empty()) {
    advanceReadOperation_(readOperations_.front());
  }
  if (!writeOperations_.empty()) {
    advanceWriteOperation_(writeOperations_.front());
  }
}

//
// Everything else
//

void Pipe::Impl::startReadingUponEstablishingPipe_() {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  if (!readOperations_.empty()) {
    advanceReadOperation_(readOperations_.front());
  }
}

void Pipe::Impl::startWritingUponEstablishingPipe_() {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  if (!writeOperations_.empty()) {
    advanceWriteOperation_(writeOperations_.front());
  }
}

void Pipe::Impl::advanceReadOperation_(ReadOperation& initialOp) {
  // Advancing one operation may unblock later ones that could have progressed
  // but were prevented from overtaking. Thus each time an operation manages to
  // advance we'll try to also advance the one after.
  for (int64_t sequenceNumber = initialOp.sequenceNumber;; ++sequenceNumber) {
    ReadOperation* opPtr = findReadOperation(sequenceNumber);
    if (opPtr == nullptr || !advanceOneReadOperation_(*opPtr)) {
      break;
    }
  }
}

bool Pipe::Impl::advanceOneReadOperation_(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  // The operations must advance in order: later operations cannot "overtake"
  // earlier ones. Thus if this operation would reach a more advanced state
  // than previous operation we won't perform the transition.
  const ReadOperation* prevOpPtr = findReadOperation(op.sequenceNumber - 1);
  const ReadOperation::State prevOpState =
      prevOpPtr != nullptr ? prevOpPtr->state : ReadOperation::FINISHED;

  // Use this helper to force a very specific structure on our checks, as
  // otherwise we'll be tempted to start merging `if`s, using `else`s, etc.
  // which seem good ideas but hide nasty pitfalls.
  auto attemptTransition = [this, &op, prevOpState](
                               ReadOperation::State from,
                               ReadOperation::State to,
                               bool cond,
                               void (Impl::*action)(ReadOperation&)) {
    if (op.state == from && cond && to <= prevOpState) {
      (this->*action)(op);
      TP_DCHECK_EQ(op.state, to);
    }
  };

  // Due to the above check, each time that an operation advances its state we
  // must check whether this unblocks some later operations that could progress
  // but weren't allowed to overtake. In order to detect whether this operation
  // is advancing we store its state at the beginning and then compare it with
  // the state at the end.
  const ReadOperation::State initialState = op.state;

  // We use a series of independent checks to advance the state, rather than a
  // switch block, because we want an operation that performs one transition to
  // be considered right away for a subsequent transition. This is needed, for
  // example, so that after reading the payloads of a message that actually has
  // no payloads we can immediately continue.

  attemptTransition(
      /*from=*/ReadOperation::UNINITIALIZED,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*cond=*/error_,
      /*action=*/&Impl::callReadDescriptorCallback_);

  attemptTransition(
      /*from=*/ReadOperation::UNINITIALIZED,
      /*to=*/ReadOperation::READING_DESCRIPTOR,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >= ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*action=*/&Impl::readDescriptorOfMessage_);

  attemptTransition(
      /*from=*/ReadOperation::READING_DESCRIPTOR,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*cond=*/error_ || op.doneReadingDescriptor,
      /*action=*/&Impl::callReadDescriptorCallback_);

  attemptTransition(
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/error_ && op.doneGettingAllocation,
      /*action=*/&Impl::callReadCallback_);

  attemptTransition(
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*to=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*cond=*/!error_ && op.doneGettingAllocation,
      /*action=*/&Impl::readPayloadsAndReceiveTensorsOfMessage);

  attemptTransition(
      /*from=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/op.numPayloadsBeingRead == 0 && op.numTensorsBeingReceived == 0,
      /*action=*/&Impl::callReadCallback_);

  // Compute return value now in case we next delete the operation.
  bool hasAdvanced = op.state != initialState;

  if (op.state == ReadOperation::FINISHED) {
    TP_DCHECK_EQ(readOperations_.front().sequenceNumber, op.sequenceNumber);
    readOperations_.pop_front();
  }

  return hasAdvanced;
}

void Pipe::Impl::advanceWriteOperation_(WriteOperation& initialOp) {
  // Advancing one operation may unblock later ones that could have progressed
  // but were prevented from overtaking. Thus each time an operation manages to
  // advance we'll try to also advance the one after.
  for (int64_t sequenceNumber = initialOp.sequenceNumber;; ++sequenceNumber) {
    WriteOperation* opPtr = findWriteOperation(sequenceNumber);
    if (opPtr == nullptr || !advanceOneWriteOperation_(*opPtr)) {
      break;
    }
  }
}

bool Pipe::Impl::advanceOneWriteOperation_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  // The operations must advance in order: later operations cannot "overtake"
  // earlier ones. Thus if this operation would reach a more advanced state
  // than previous operation we won't perform the transition.
  const WriteOperation* prevOpPtr = findWriteOperation(op.sequenceNumber - 1);
  const WriteOperation::State prevOpState =
      prevOpPtr != nullptr ? prevOpPtr->state : WriteOperation::FINISHED;

  // Use this helper to force a very specific structure on our checks, as
  // otherwise we'll be tempted to start merging `if`s, using `else`s, etc.
  // which seem good ideas but hide nasty pitfalls.
  auto attemptTransition = [this, &op, prevOpState](
                               WriteOperation::State from,
                               WriteOperation::State to,
                               bool cond,
                               void (Impl::*action)(WriteOperation&)) {
    if (op.state == from && cond && to <= prevOpState) {
      (this->*action)(op);
      TP_DCHECK_EQ(op.state, to);
    }
  };

  // Due to the above check, each time that an operation advances its state we
  // must check whether this unblocks some later operations that could progress
  // but weren't allowed to overtake. In order to detect whether this operation
  // is advancing we store its state at the beginning and then compare it with
  // the state at the end.
  const WriteOperation::State initialState = op.state;

  // We use a series of independent checks to advance the state, rather than a
  // switch block, because we want an operation that performs one transition to
  // be considered right away for a subsequent transition. This is needed, for
  // example, so that after writing the payloads of a message that actually has
  // no payloads we can immediately continue.

  attemptTransition(
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_,
      /*action=*/&Impl::callWriteCallback_);

  attemptTransition(
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
      /*cond=*/!error_ && state_ == ESTABLISHED,
      /*action=*/&Impl::sendTensorsOfMessage_);

  attemptTransition(
      /*from=*/WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_ && op.numTensorDescriptorsBeingCollected == 0 &&
          op.numTensorsBeingSent == 0,
      /*action=*/&Impl::callWriteCallback_);

  attemptTransition(
      /*from=*/WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
      /*to=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*cond=*/!error_ && op.numTensorDescriptorsBeingCollected == 0,
      /*action=*/&Impl::writeDescriptorAndPayloadsOfMessage_);

  attemptTransition(
      /*from=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/op.numPayloadsBeingWritten == 0 && op.numTensorsBeingSent == 0,
      /*action=*/&Impl::callWriteCallback_);

  // Compute return value now in case we next delete the operation.
  bool hasAdvanced = op.state != initialState;

  if (op.state == WriteOperation::FINISHED) {
    TP_DCHECK_EQ(writeOperations_.front().sequenceNumber, op.sequenceNumber);
    writeOperations_.pop_front();
  }

  return hasAdvanced;
}

void Pipe::Impl::readDescriptorOfMessage_(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  TP_DCHECK_EQ(op.state, ReadOperation::UNINITIALIZED);
  op.state = ReadOperation::READING_DESCRIPTOR;

  TP_VLOG(2) << "Pipe " << id_ << " is reading descriptor of message #"
             << op.sequenceNumber;

  TP_DCHECK_EQ(connectionState_, AWAITING_DESCRIPTOR);
  TP_DCHECK_EQ(messageBeingReadFromConnection_, op.sequenceNumber);
  auto pbPacketIn = std::make_shared<proto::Packet>();
  TP_VLOG(3) << "Pipe " << id_ << " is reading proto (message descriptor #"
             << op.sequenceNumber << ")";
  connection_->read(
      *pbPacketIn, lazyCallbackWrapper_([&op, pbPacketIn](Impl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done reading proto (message descriptor #"
                   << op.sequenceNumber << ")";
        impl.onReadOfMessageDescriptor_(op, *pbPacketIn);
      }));
  connectionState_ = AWAITING_PAYLOADS;
}

void Pipe::Impl::sendTensorsOfMessage_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  TP_DCHECK_EQ(op.state, WriteOperation::UNINITIALIZED);
  op.state = WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS;

  TP_VLOG(2) << "Pipe " << id_ << " is sending tensors of message #"
             << op.sequenceNumber;

  for (int tensorIdx = 0; tensorIdx < op.message.tensors.size(); ++tensorIdx) {
    const auto& tensor = op.message.tensors[tensorIdx];
    bool foundAChannel = false;
    for (const auto& channelContextIter : context_->getOrderedChannels()) {
      const std::string& channelName = std::get<0>(channelContextIter.second);

      auto channelIter = channels_.find(channelName);
      if (channelIter == channels_.cend()) {
        continue;
      }
      channel::Channel& channel = *(channelIter->second);

      TP_VLOG(3) << "Pipe " << id_ << " is sending tensor #"
                 << op.sequenceNumber << "." << tensorIdx;
      channel.send(
          tensor.data,
          tensor.length,
          eagerCallbackWrapper_([&op, tensorIdx](
                                    Impl& impl,
                                    channel::Channel::TDescriptor descriptor) {
            TP_VLOG(3) << "Pipe " << impl.id_ << " got tensor descriptor #"
                       << op.sequenceNumber << "." << tensorIdx;
            impl.onDescriptorOfTensor_(op, tensorIdx, std::move(descriptor));
          }),
          eagerCallbackWrapper_([&op, tensorIdx](Impl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_ << " done sending tensor #"
                       << op.sequenceNumber << "." << tensorIdx;
            impl.onSendOfTensor_(op);
          }));
      op.tensors.push_back(WriteOperation::Tensor{channelName});
      ++op.numTensorDescriptorsBeingCollected;
      ++op.numTensorsBeingSent;

      foundAChannel = true;
      break;
    }
    TP_THROW_ASSERT_IF(!foundAChannel);
  }
}

void Pipe::Impl::writeDescriptorAndPayloadsOfMessage_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  TP_DCHECK_EQ(
      op.state, WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS);
  op.state = WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS;

  TP_VLOG(2) << "Pipe " << id_
             << " is writing descriptor and payloads of message #"
             << op.sequenceNumber;

  std::shared_ptr<proto::Packet> pbPacketOut = makeDescriptorForMessage(op);

  TP_VLOG(3) << "Pipe " << id_ << " is writing proto (message descriptor #"
             << op.sequenceNumber << ")";
  connection_->write(
      *pbPacketOut,
      lazyCallbackWrapper_(
          [sequenceNumber{op.sequenceNumber}, pbPacketOut](Impl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_
                       << " done writing proto (message descriptor #"
                       << sequenceNumber << ")";
          }));

  for (size_t payloadIdx = 0; payloadIdx < op.message.payloads.size();
       payloadIdx++) {
    Message::Payload& payload = op.message.payloads[payloadIdx];
    TP_VLOG(3) << "Pipe " << id_ << " is writing payload #" << op.sequenceNumber
               << "." << payloadIdx;
    connection_->write(
        payload.data,
        payload.length,
        eagerCallbackWrapper_([&op, payloadIdx](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done writing payload #"
                     << op.sequenceNumber << "." << payloadIdx;
          impl.onWriteOfPayload_(op);
        }));
    ++op.numPayloadsBeingWritten;
  }
}

void Pipe::Impl::onReadWhileServerWaitingForBrochure_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_BROCHURE);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kBrochure);
  const proto::Brochure& pbBrochure = pbPacketIn.brochure();

  auto pbPacketOut = std::make_shared<proto::Packet>();
  proto::BrochureAnswer* pbBrochureAnswer =
      pbPacketOut->mutable_brochure_answer();
  bool needToWaitForConnections = false;

  bool foundATransport = false;
  for (const auto& transportContextIter : context_->getOrderedTransports()) {
    const std::string& transportName = std::get<0>(transportContextIter.second);
    const transport::Context& transportContext =
        *(std::get<1>(transportContextIter.second));

    // This pipe's listener might not have an address for that transport.
    const std::map<std::string, std::string>& addresses =
        listener_->addresses();
    const auto addressIter = addresses.find(transportName);
    if (addressIter == addresses.cend()) {
      continue;
    }
    const std::string& address = addressIter->second;

    const auto pbTransportAdvertisementIter =
        pbBrochure.transport_advertisement().find(transportName);
    if (pbTransportAdvertisementIter ==
        pbBrochure.transport_advertisement().cend()) {
      continue;
    }
    const proto::TransportAdvertisement& pbTransportAdvertisement =
        pbTransportAdvertisementIter->second;
    const std::string& domainDescriptor =
        pbTransportAdvertisement.domain_descriptor();
    if (domainDescriptor != transportContext.domainDescriptor()) {
      continue;
    }

    pbBrochureAnswer->set_transport(transportName);
    pbBrochureAnswer->set_address(address);

    if (transportName != transport_) {
      transport_ = transportName;
      TP_DCHECK(!registrationId_.has_value());
      TP_VLOG(3) << "Pipe " << id_
                 << " is requesting connection (as replacement)";
      uint64_t token =
          listener_->registerConnectionRequest(lazyCallbackWrapper_(
              [](Impl& impl,
                 std::string transport,
                 std::shared_ptr<transport::Connection> connection) {
                TP_VLOG(3) << "Pipe " << impl.id_
                           << " done requesting connection (as replacement)";
                impl.onAcceptWhileServerWaitingForConnection_(
                    std::move(transport), std::move(connection));
              }));
      registrationId_.emplace(token);
      needToWaitForConnections = true;
      pbBrochureAnswer->set_registration_id(token);
    }

    foundATransport = true;
    break;
  }
  TP_THROW_ASSERT_IF(!foundATransport);

  auto pbAllChannelSelections = pbBrochureAnswer->mutable_channel_selection();
  for (const auto& channelContextIter : context_->getOrderedChannels()) {
    const std::string& channelName = std::get<0>(channelContextIter.second);
    const channel::Context& channelContext =
        *(std::get<1>(channelContextIter.second));

    const auto pbChannelAdvertisementIter =
        pbBrochure.channel_advertisement().find(channelName);
    if (pbChannelAdvertisementIter ==
        pbBrochure.channel_advertisement().cend()) {
      continue;
    }
    const proto::ChannelAdvertisement& pbChannelAdvertisement =
        pbChannelAdvertisementIter->second;
    const std::string& domainDescriptor =
        pbChannelAdvertisement.domain_descriptor();
    if (domainDescriptor != channelContext.domainDescriptor()) {
      continue;
    }

    TP_VLOG(3) << "Pipe " << id_ << " is requesting connection (for channel "
               << channelName << ")";
    uint64_t token = listener_->registerConnectionRequest(lazyCallbackWrapper_(
        [channelName](
            Impl& impl,
            std::string transport,
            std::shared_ptr<transport::Connection> connection) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done requesting connection (for channel "
                     << channelName << ")";
          impl.onAcceptWhileServerWaitingForChannel_(
              channelName, std::move(transport), std::move(connection));
        }));
    channelRegistrationIds_[channelName] = token;
    needToWaitForConnections = true;
    proto::ChannelSelection* pbChannelSelection =
        &(*pbAllChannelSelections)[channelName];
    pbChannelSelection->set_registration_id(token);
  }

  TP_VLOG(3) << "Pipe " << id_ << " is writing proto (brochure answer)";
  connection_->write(
      *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done writing proto (brochure answer)";
      }));

  if (!needToWaitForConnections) {
    state_ = ESTABLISHED;
    startReadingUponEstablishingPipe_();
    startWritingUponEstablishingPipe_();
  } else {
    state_ = SERVER_WAITING_FOR_CONNECTIONS;
  }
}

void Pipe::Impl::onReadWhileClientWaitingForBrochureAnswer_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, CLIENT_WAITING_FOR_BROCHURE_ANSWER);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kBrochureAnswer);

  const proto::BrochureAnswer& pbBrochureAnswer = pbPacketIn.brochure_answer();
  const std::string& transport = pbBrochureAnswer.transport();
  std::string address = pbBrochureAnswer.address();
  std::shared_ptr<transport::Context> transportContext =
      context_->getTransport(transport);

  if (transport != transport_) {
    TP_VLOG(3) << "Pipe " << id_ << " is opening connection (as replacement)";
    std::shared_ptr<transport::Connection> connection =
        transportContext->connect(address);
    connection->setId(id_ + ".tr_" + transport);
    auto pbPacketOut = std::make_shared<proto::Packet>();
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut->mutable_requested_connection();
    uint64_t token = pbBrochureAnswer.registration_id();
    pbRequestedConnection->set_registration_id(token);
    TP_VLOG(3) << "Pipe " << id_ << " is writing proto (requested connection)";
    connection->write(
        *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing proto (requested connection)";
        }));

    transport_ = transport;
    connection_ = std::move(connection);
  }

  for (const auto& pbChannelSelectionIter :
       pbBrochureAnswer.channel_selection()) {
    const std::string& channelName = pbChannelSelectionIter.first;
    const proto::ChannelSelection& pbChannelSelection =
        pbChannelSelectionIter.second;

    std::shared_ptr<channel::Context> channelContext =
        context_->getChannel(channelName);

    TP_VLOG(3) << "Pipe " << id_ << " is opening connection (for channel "
               << channelName << ")";
    std::shared_ptr<transport::Connection> connection =
        transportContext->connect(address);
    connection->setId(id_ + ".ch_" + channelName);

    auto pbPacketOut = std::make_shared<proto::Packet>();
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut->mutable_requested_connection();
    uint64_t token = pbChannelSelection.registration_id();
    pbRequestedConnection->set_registration_id(token);
    TP_VLOG(3) << "Pipe " << id_ << " is writing proto (requested connection)";
    connection->write(
        *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing proto (requested connection)";
        }));

    std::shared_ptr<channel::Channel> channel = channelContext->createChannel(
        std::move(connection), channel::Channel::Endpoint::kConnect);
    channel->setId(id_ + ".ch_" + channelName);
    channels_.emplace(channelName, std::move(channel));
  }

  state_ = ESTABLISHED;
  startReadingUponEstablishingPipe_();
  startWritingUponEstablishingPipe_();
}

void Pipe::Impl::onAcceptWhileServerWaitingForConnection_(
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  TP_DCHECK(registrationId_.has_value());
  listener_->unregisterConnectionRequest(registrationId_.value());
  registrationId_.reset();
  receivedConnection->setId(id_ + ".tr_" + receivedTransport);
  TP_DCHECK_EQ(transport_, receivedTransport);
  connection_.reset();
  connection_ = std::move(receivedConnection);

  if (!registrationId_.has_value() && channelRegistrationIds_.empty()) {
    state_ = ESTABLISHED;
    startReadingUponEstablishingPipe_();
    startWritingUponEstablishingPipe_();
  }
}

void Pipe::Impl::onAcceptWhileServerWaitingForChannel_(
    std::string channelName,
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  auto channelRegistrationIdIter = channelRegistrationIds_.find(channelName);
  TP_DCHECK(channelRegistrationIdIter != channelRegistrationIds_.end());
  listener_->unregisterConnectionRequest(channelRegistrationIdIter->second);
  channelRegistrationIds_.erase(channelRegistrationIdIter);
  receivedConnection->setId(id_ + ".ch_" + channelName);

  TP_DCHECK_EQ(transport_, receivedTransport);
  auto channelIter = channels_.find(channelName);
  TP_DCHECK(channelIter == channels_.end());

  std::shared_ptr<channel::Context> channelContext =
      context_->getChannel(channelName);

  std::shared_ptr<channel::Channel> channel = channelContext->createChannel(
      std::move(receivedConnection), channel::Channel::Endpoint::kListen);
  channel->setId(id_ + ".ch_" + channelName);
  channels_.emplace(channelName, std::move(channel));

  if (!registrationId_.has_value() && channelRegistrationIds_.empty()) {
    state_ = ESTABLISHED;
    startReadingUponEstablishingPipe_();
    startWritingUponEstablishingPipe_();
  }
}

void Pipe::Impl::onReadOfMessageDescriptor_(
    ReadOperation& op,
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  TP_DCHECK_EQ(op.state, ReadOperation::READING_DESCRIPTOR);
  parseDescriptorOfMessage(op, pbPacketIn);
  op.doneReadingDescriptor = true;

  advanceReadOperation_(op);
}

void Pipe::Impl::onDescriptorOfTensor_(
    WriteOperation& op,
    int64_t tensorIdx,
    channel::Channel::TDescriptor descriptor) {
  TP_DCHECK(loop_.inLoop());

  TP_DCHECK_EQ(
      op.state, WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS);
  TP_DCHECK_LT(tensorIdx, op.tensors.size());
  op.tensors[tensorIdx].descriptor = std::move(descriptor);
  --op.numTensorDescriptorsBeingCollected;

  advanceWriteOperation_(op);
}

void Pipe::Impl::onReadOfPayload_(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());

  TP_DCHECK_EQ(op.state, ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS);
  op.numPayloadsBeingRead--;

  advanceReadOperation_(op);
}

void Pipe::Impl::onRecvOfTensor_(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());

  TP_DCHECK_EQ(op.state, ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS);
  op.numTensorsBeingReceived--;

  advanceReadOperation_(op);
}

void Pipe::Impl::onWriteOfPayload_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());

  TP_DCHECK_EQ(op.state, WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS);
  op.numPayloadsBeingWritten--;

  advanceWriteOperation_(op);
}

void Pipe::Impl::onSendOfTensor_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());

  TP_DCHECK_GE(
      op.state, WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS);
  TP_DCHECK_LE(op.state, WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS);
  op.numTensorsBeingSent--;

  advanceWriteOperation_(op);
}

ReadOperation* Pipe::Impl::findReadOperation(int64_t sequenceNumber) {
  if (readOperations_.empty()) {
    return nullptr;
  }
  int64_t offset = sequenceNumber - readOperations_.front().sequenceNumber;
  if (offset < 0 || offset >= readOperations_.size()) {
    return nullptr;
  }
  ReadOperation& op = readOperations_[offset];
  TP_DCHECK_EQ(op.sequenceNumber, sequenceNumber);
  return &op;
}

WriteOperation* Pipe::Impl::findWriteOperation(int64_t sequenceNumber) {
  if (writeOperations_.empty()) {
    return nullptr;
  }
  int64_t offset = sequenceNumber - writeOperations_.front().sequenceNumber;
  if (offset < 0 || offset >= writeOperations_.size()) {
    return nullptr;
  }
  WriteOperation& op = writeOperations_[offset];
  TP_DCHECK_EQ(op.sequenceNumber, sequenceNumber);
  return &op;
}

} // namespace tensorpipe
