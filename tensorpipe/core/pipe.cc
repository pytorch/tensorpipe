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

  Pipe::read_descriptor_callback_fn readDescriptorCallback;

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

  Message message;
  Pipe::read_callback_fn readCallback;
  int64_t numPayloadsStillBeingRead{0};
  int64_t numTensorsStillBeingReceived{0};
};

struct WriteOperation {
  int64_t sequenceNumber{-1};

  Message message;
  Pipe::write_callback_fn writeCallback;
  bool startedWritingPayloads{false};
  bool startedSendingTensors{false};
  int64_t numPayloadsStillBeingWritten{0};
  int64_t numTensorDescriptorsStillBeingCollected{0};
  int64_t numTensorsStillBeingSent{0};
  struct Tensor {
    std::string channelName;
    channel::Channel::TDescriptor descriptor;
  };
  std::vector<Tensor> tensors;
};

} // namespace

class Pipe::Impl : public std::enable_shared_from_this<Pipe::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::string id,
      const std::string& url);

  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::shared_ptr<Listener::PrivateIface> listener,
      std::string id,
      std::string transport,
      std::shared_ptr<transport::Connection> connection);

  // Called by the pipe's constructor.
  void init();

  void readDescriptor(read_descriptor_callback_fn);
  void read(Message, read_callback_fn);
  void write(Message, write_callback_fn);

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
  enum ConnectionState { NEXT_UP_IS_DESCRIPTOR, NEXT_UP_IS_PAYLOADS };
  ConnectionState connectionState_{NEXT_UP_IS_DESCRIPTOR};
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

  void triggerReadyCallbacks_();

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

  void readDescriptorOfMessage_(ReadOperation&);
  void sendTensorsOfMessage_(WriteOperation&);
  void writeMessage_(WriteOperation&);
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
  void onDescriptorOfTensor_(int64_t, int64_t, channel::Channel::TDescriptor);
  void onReadOfPayload_(int64_t);
  void onRecvOfTensor_(int64_t);
  void onWriteOfPayload_(int64_t);
  void onSendOfTensor_(int64_t);

  void checkForMessagesDoneCollectingTensorDescriptors_();

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
    const std::string& url)
    : impl_(std::make_shared<Impl>(std::move(context), std::move(id), url)) {
  impl_->init();
}

Pipe::Pipe(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<Listener::PrivateIface> listener,
    std::string id,
    std::string transport,
    std::shared_ptr<transport::Connection> connection)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(listener),
          std::move(id),
          std::move(transport),
          std::move(connection))) {
  impl_->init();
}

Pipe::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::string id,
    const std::string& url)
    : state_(CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE),
      context_(std::move(context)),
      id_(std::move(id)),
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
    std::string transport,
    std::shared_ptr<transport::Connection> connection)
    : state_(SERVER_WAITING_FOR_BROCHURE),
      context_(std::move(context)),
      listener_(std::move(listener)),
      id_(std::move(id)),
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

  int64_t sequenceNumber = nextMessageBeingRead_++;
  TP_VLOG(1) << "Pipe " << id_ << " received a readDescriptor request (#"
             << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextReadDescriptorCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a readDescriptor callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG(1) << "Pipe " << id_ << " done calling a readDescriptor callback (#"
               << sequenceNumber << ")";
  };

  readOperations_.emplace_back();
  ReadOperation& op = readOperations_.back();
  op.sequenceNumber = sequenceNumber;
  op.readDescriptorCallback = std::move(fn);

  if (error_) {
    triggerReadyCallbacks_();
    return;
  }

  if (state_ == ESTABLISHED && connectionState_ == NEXT_UP_IS_DESCRIPTOR &&
      messageBeingReadFromConnection_ == sequenceNumber) {
    readDescriptorOfMessage_(op);
  }
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

  // Other sanity checks that must pass unless the user really messed up.
  size_t numPayloads = message.payloads.size();
  TP_THROW_ASSERT_IF(numPayloads != op.payloads.size());
  for (size_t payloadIdx = 0; payloadIdx < numPayloads; payloadIdx++) {
    Message::Payload& payload = message.payloads[payloadIdx];
    ReadOperation::Payload& payloadBeingAllocated = op.payloads[payloadIdx];
    TP_DCHECK_GE(payloadBeingAllocated.length, 0);
    TP_THROW_ASSERT_IF(payload.length != payloadBeingAllocated.length);
  }
  size_t numTensors = message.tensors.size();
  TP_THROW_ASSERT_IF(numTensors != op.tensors.size());
  for (size_t tensorIdx = 0; tensorIdx < numTensors; tensorIdx++) {
    Message::Tensor& tensor = message.tensors[tensorIdx];
    ReadOperation::Tensor& tensorBeingAllocated = op.tensors[tensorIdx];
    TP_DCHECK_GE(tensorBeingAllocated.length, 0);
    TP_THROW_ASSERT_IF(tensor.length != tensorBeingAllocated.length);
  }

  int64_t sequenceNumber = op.sequenceNumber;
  TP_VLOG(1) << "Pipe " << id_ << " received a read request (#"
             << sequenceNumber << ", contaning " << numPayloads
             << " payloads and " << numTensors << " tensors)";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextReadCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a read callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG(1) << "Pipe " << id_ << " done calling a read callback (#"
               << sequenceNumber << ")";
  };

  op.message = std::move(message);
  op.readCallback = std::move(fn);

  if (error_) {
    triggerReadyCallbacks_();
    return;
  }

  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_PAYLOADS);
  TP_DCHECK_EQ(messageBeingReadFromConnection_, sequenceNumber);
  for (size_t payloadIdx = 0; payloadIdx < numPayloads; payloadIdx++) {
    Message::Payload& payload = op.message.payloads[payloadIdx];
    TP_VLOG(3) << "Pipe " << id_ << " is reading payload #" << sequenceNumber
               << "." << payloadIdx;
    connection_->read(
        payload.data,
        payload.length,
        eagerCallbackWrapper_(
            [sequenceNumber, payloadIdx](
                Impl& impl, const void* /* unused */, size_t /* unused */) {
              TP_VLOG(3) << "Pipe " << impl.id_ << " done reading payload #"
                         << sequenceNumber << "." << payloadIdx;
              impl.onReadOfPayload_(sequenceNumber);
            }));
    ++op.numPayloadsStillBeingRead;
  }
  connectionState_ = NEXT_UP_IS_DESCRIPTOR;
  ++messageBeingReadFromConnection_;

  for (size_t tensorIdx = 0; tensorIdx < numTensors; tensorIdx++) {
    Message::Tensor& tensor = op.message.tensors[tensorIdx];
    ReadOperation::Tensor& tensorBeingAllocated = op.tensors[tensorIdx];
    std::shared_ptr<channel::Channel> channel =
        channels_.at(tensorBeingAllocated.channelName);
    TP_VLOG(3) << "Pipe " << id_ << " is receiving tensor #" << sequenceNumber
               << "." << tensorIdx;
    channel->recv(
        std::move(tensorBeingAllocated.descriptor),
        tensor.data,
        tensor.length,
        eagerCallbackWrapper_([sequenceNumber, tensorIdx](Impl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done receiving tensor #"
                     << sequenceNumber << "." << tensorIdx;
          impl.onRecvOfTensor_(sequenceNumber);
        }));
    ++op.numTensorsStillBeingReceived;
  }

  ReadOperation* nextOpPtr = findReadOperation(sequenceNumber + 1);
  if (nextOpPtr != nullptr) {
    readDescriptorOfMessage_(*nextOpPtr);
  }

  if (numPayloads == 0 && numTensors == 0) {
    triggerReadyCallbacks_();
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

  int64_t sequenceNumber = nextMessageBeingWritten_++;
  TP_VLOG(1) << "Pipe " << id_ << " received a write request (#"
             << sequenceNumber << ", contaning " << message.payloads.size()
             << " payloads and " << message.tensors.size() << " tensors)";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextWriteCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a write callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG(1) << "Pipe " << id_ << " done calling a write callback (#"
               << sequenceNumber << ")";
  };

  writeOperations_.push_back(
      WriteOperation{sequenceNumber, std::move(message), std::move(fn)});

  if (error_) {
    triggerReadyCallbacks_();
    return;
  }

  if (state_ == ESTABLISHED) {
    sendTensorsOfMessage_(writeOperations_.back());
  }
}

//
// Helpers to schedule our callbacks into user code
//

void Pipe::Impl::triggerReadyCallbacks_() {
  TP_DCHECK(loop_.inLoop());

  while (true) {
    if (!readOperations_.empty() &&
        nextMessageAskingForAllocation_ <=
            readOperations_.back().sequenceNumber) {
      if (error_) {
        ReadOperation& op = *findReadOperation(nextMessageAskingForAllocation_);
        op.readDescriptorCallback(error_, Message());
        // Reset callback to release the resources it was holding.
        op.readDescriptorCallback = nullptr;
        ++nextMessageAskingForAllocation_;
        continue;
      }
    }
    if (!readOperations_.empty() &&
        readOperations_.front().sequenceNumber <
            nextMessageGettingAllocation_) {
      ReadOperation& op = readOperations_.front();
      // We don't check for error, because even in case of error we still must
      // wait for all transport/channel callbacks to return to be sure that no
      // other thread is working on the data.
      if (op.numPayloadsStillBeingRead == 0 &&
          op.numTensorsStillBeingReceived == 0) {
        op.readCallback(error_, std::move(op.message));
        readOperations_.pop_front();
        continue;
      }
    }
    if (!writeOperations_.empty()) {
      WriteOperation& op = writeOperations_.front();
      // If the message has already been written we don't check for error,
      // because even in case of error we still must wait for all transport/
      // channel callbacks to return to be sure that no other thread is working
      // on the data.
      bool doneWritingData =
          op.startedWritingPayloads && op.numPayloadsStillBeingWritten == 0;
      bool blockedWritingData =
          op.startedWritingPayloads && op.numPayloadsStillBeingWritten > 0;
      bool doneSendingTensors = op.startedSendingTensors &&
          op.numTensorDescriptorsStillBeingCollected == 0 &&
          op.numTensorsStillBeingSent == 0;
      bool blockedWritingTensors = op.startedSendingTensors &&
          (op.numTensorDescriptorsStillBeingCollected > 0 ||
           op.numTensorsStillBeingSent > 0);
      if ((error_ && !blockedWritingData && !blockedWritingTensors) ||
          (doneWritingData && doneSendingTensors)) {
        op.writeCallback(error_, std::move(op.message));
        writeOperations_.pop_front();
        continue;
      }
    }
    break;
  }
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

  // TODO Close all connections and channels.

  if (registrationId_.has_value()) {
    listener_->unregisterConnectionRequest(registrationId_.value());
    registrationId_.reset();
  }
  for (const auto& iter : channelRegistrationIds_) {
    listener_->unregisterConnectionRequest(iter.second);
  }
  channelRegistrationIds_.clear();

  triggerReadyCallbacks_();
}

//
// Everything else
//

void Pipe::Impl::startReadingUponEstablishingPipe_() {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  if (!readOperations_.empty()) {
    readDescriptorOfMessage_(readOperations_.front());
  }
}

void Pipe::Impl::startWritingUponEstablishingPipe_() {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  for (WriteOperation& op : writeOperations_) {
    sendTensorsOfMessage_(op);
  }
}

void Pipe::Impl::readDescriptorOfMessage_(ReadOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
  TP_DCHECK_EQ(messageBeingReadFromConnection_, op.sequenceNumber);
  auto pbPacketIn = std::make_shared<proto::Packet>();
  TP_VLOG(3) << "Pipe " << id_ << " is reading proto (message descriptor #"
             << op.sequenceNumber << ")";
  connection_->read(
      *pbPacketIn,
      lazyCallbackWrapper_(
          [sequenceNumber{op.sequenceNumber}, pbPacketIn](Impl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_
                       << " done reading proto (message descriptor #"
                       << sequenceNumber << ")";
            impl.onReadOfMessageDescriptor_(*pbPacketIn);
          }));
  connectionState_ = NEXT_UP_IS_PAYLOADS;
}

void Pipe::Impl::sendTensorsOfMessage_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!op.startedSendingTensors);
  op.startedSendingTensors = true;
  TP_VLOG(2) << "Pipe " << id_ << " is sending tensors of message #"
             << op.sequenceNumber;

  if (op.message.tensors.size() == 0) {
    checkForMessagesDoneCollectingTensorDescriptors_();
    return;
  }

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
          eagerCallbackWrapper_(
              [sequenceNumber{op.sequenceNumber}, tensorIdx](
                  Impl& impl, channel::Channel::TDescriptor descriptor) {
                TP_VLOG(3) << "Pipe " << impl.id_ << " got tensor descriptor #"
                           << sequenceNumber << "." << tensorIdx;
                impl.onDescriptorOfTensor_(
                    sequenceNumber, tensorIdx, std::move(descriptor));
              }),
          eagerCallbackWrapper_(
              [sequenceNumber{op.sequenceNumber}, tensorIdx](Impl& impl) {
                TP_VLOG(3) << "Pipe " << impl.id_ << " done sending tensor #"
                           << sequenceNumber << "." << tensorIdx;
                impl.onSendOfTensor_(sequenceNumber);
              }));
      op.tensors.push_back(WriteOperation::Tensor{channelName});
      ++op.numTensorDescriptorsStillBeingCollected;
      ++op.numTensorsStillBeingSent;

      foundAChannel = true;
      break;
    }
    TP_THROW_ASSERT_IF(!foundAChannel);
  }
}

void Pipe::Impl::writeMessage_(WriteOperation& op) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!op.startedWritingPayloads);
  op.startedWritingPayloads = true;
  TP_VLOG(2) << "Pipe " << id_
             << " is writing descriptor and payloads of message #"
             << op.sequenceNumber;

  auto pbPacketOut = std::make_shared<proto::Packet>();
  proto::MessageDescriptor* pbMessageDescriptor =
      pbPacketOut->mutable_message_descriptor();

  pbMessageDescriptor->set_metadata(op.message.metadata);

  for (int payloadIdx = 0; payloadIdx < op.message.payloads.size();
       ++payloadIdx) {
    const auto& payload = op.message.payloads[payloadIdx];
    proto::MessageDescriptor::PayloadDescriptor* pbPayloadDescriptor =
        pbMessageDescriptor->add_payload_descriptors();
    pbPayloadDescriptor->set_size_in_bytes(payload.length);
    pbPayloadDescriptor->set_metadata(payload.metadata);
  }

  TP_DCHECK_EQ(op.message.tensors.size(), op.tensors.size());
  for (int tensorIdx = 0; tensorIdx < op.tensors.size(); ++tensorIdx) {
    const auto& tensor = op.message.tensors[tensorIdx];
    const auto& otherTensor = op.tensors[tensorIdx];
    proto::MessageDescriptor::TensorDescriptor* pbTensorDescriptor =
        pbMessageDescriptor->add_tensor_descriptors();
    pbTensorDescriptor->set_device_type(proto::DeviceType::DEVICE_TYPE_CPU);
    pbTensorDescriptor->set_size_in_bytes(tensor.length);
    pbTensorDescriptor->set_metadata(tensor.metadata);
    pbTensorDescriptor->set_channel_name(otherTensor.channelName);
    // FIXME In principle we could move here.
    pbTensorDescriptor->set_channel_descriptor(otherTensor.descriptor);
  }

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
        eagerCallbackWrapper_(
            [sequenceNumber{op.sequenceNumber}, payloadIdx](Impl& impl) {
              TP_VLOG(3) << "Pipe " << impl.id_ << " done writing payload #"
                         << sequenceNumber << "." << payloadIdx;
              impl.onWriteOfPayload_(sequenceNumber);
            }));
    ++op.numPayloadsStillBeingWritten;
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

void Pipe::Impl::onReadOfMessageDescriptor_(const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  ReadOperation* opPtr = findReadOperation(nextMessageAskingForAllocation_);
  TP_DCHECK(opPtr != nullptr);
  ReadOperation& op = *opPtr;
  ++nextMessageAskingForAllocation_;

  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kMessageDescriptor);
  const proto::MessageDescriptor& pbMessageDescriptor =
      pbPacketIn.message_descriptor();

  Message message;

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

  op.readDescriptorCallback(Error::kSuccess, std::move(message));
  // Reset callback to release the resources it was holding.
  op.readDescriptorCallback = nullptr;
}

void Pipe::Impl::onDescriptorOfTensor_(
    int64_t sequenceNumber,
    int64_t tensorIdx,
    channel::Channel::TDescriptor descriptor) {
  TP_DCHECK(loop_.inLoop());

  WriteOperation* opPtr = findWriteOperation(sequenceNumber);
  TP_DCHECK(opPtr != nullptr) << "Couldn't find message #" << sequenceNumber;
  WriteOperation& op = *opPtr;

  TP_DCHECK(op.startedSendingTensors);
  TP_DCHECK_LT(tensorIdx, op.tensors.size());
  op.tensors[tensorIdx].descriptor = std::move(descriptor);
  --op.numTensorDescriptorsStillBeingCollected;
  checkForMessagesDoneCollectingTensorDescriptors_();
}

void Pipe::Impl::onReadOfPayload_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());

  ReadOperation* opPtr = findReadOperation(sequenceNumber);
  TP_DCHECK(opPtr != nullptr) << "Couldn't find message #" << sequenceNumber;
  ReadOperation& op = *opPtr;

  op.numPayloadsStillBeingRead--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onRecvOfTensor_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());

  ReadOperation* opPtr = findReadOperation(sequenceNumber);
  TP_DCHECK(opPtr != nullptr) << "Couldn't find message #" << sequenceNumber;
  ReadOperation& op = *opPtr;

  op.numTensorsStillBeingReceived--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onWriteOfPayload_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());

  WriteOperation* opPtr = findWriteOperation(sequenceNumber);
  TP_DCHECK(opPtr != nullptr) << "Couldn't find message #" << sequenceNumber;
  WriteOperation& op = *opPtr;

  op.numPayloadsStillBeingWritten--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onSendOfTensor_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());

  WriteOperation* opPtr = findWriteOperation(sequenceNumber);
  TP_DCHECK(opPtr != nullptr) << "Couldn't find message #" << sequenceNumber;
  WriteOperation& op = *opPtr;

  op.numTensorsStillBeingSent--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::checkForMessagesDoneCollectingTensorDescriptors_() {
  // FIXME It's not great to iterate over all pending messages each time. If
  // this was a map indexed by sequence number we could just store the sequence
  // number of the last message we had written and resume looking from there.
  for (WriteOperation& op : writeOperations_) {
    // An outgoing message is processed in three stages, with messages coming
    // later in the queue being at an earlier stage than earlier messages.
    //
    // O  startedSendingTensors == false,
    // |    && numTensorDescriptorsStillBeingCollected == 0
    // |    && startedWritingPayloads == false
    // |
    // |  sendTensorsOfMessage_ is called on it
    // V
    // O  startedSendingTensors == true,
    // |    && numTensorDescriptorsStillBeingCollected > 0
    // |    && startedWritingPayloads == false
    // |
    // |  the descriptor callbacks fire
    // V
    // O  startedSendingTensors == true,
    // |    && numTensorDescriptorsStillBeingCollected == 0
    // |    && startedWritingPayloads == false
    // |
    // |  writeMessage_ is called on it
    // V
    // O  startedSendingTensors == true,
    //      && numTensorDescriptorsStillBeingCollected == 0
    //      && startedWritingPayloads == true
    //
    // (The state diagram actually goes on, with numPayloadsStillBeingWritten
    // and numTensorsStillBeingSent going from > 0 to == 0).
    if (op.startedWritingPayloads) {
      continue;
    }
    if (op.startedSendingTensors &&
        op.numTensorDescriptorsStillBeingCollected == 0) {
      writeMessage_(op);
    } else {
      break;
    }
  }
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
