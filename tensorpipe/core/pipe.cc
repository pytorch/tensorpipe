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

  enum ConnectionState { NEXT_UP_IS_DESCRIPTOR, NEXT_UP_IS_PAYLOADS };

  ConnectionState connectionState_{NEXT_UP_IS_DESCRIPTOR};

  struct MessageBeingExpected {
    int64_t sequenceNumber{-1};
    read_descriptor_callback_fn callback;
  };

  struct MessageBeingAllocated {
    int64_t sequenceNumber{-1};
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
  };

  struct MessageBeingRead {
    int64_t sequenceNumber{-1};
    Message message;
    read_callback_fn callback;
    int64_t numPayloadsStillBeingRead{0};
    int64_t numTensorsStillBeingReceived{0};
  };

  struct MessageBeingWritten {
    int64_t sequenceNumber{-1};
    Message message;
    write_callback_fn callback;
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

  uint64_t nextMessageBeingRead_{0};
  std::deque<MessageBeingExpected> messagesBeingExpected_;
  uint64_t nextReadDescriptorCallbackToCall_{0};
  std::deque<MessageBeingAllocated> messagesBeingAllocated_;
  std::deque<MessageBeingRead> messagesBeingRead_;
  uint64_t nextReadCallbackToCall_{0};

  uint64_t nextMessageBeingWritten_{0};
  std::deque<MessageBeingWritten> messagesBeingWritten_;
  uint64_t nextWriteCallbackToCall_{0};

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

  void doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  void sendTensorsOfMessage_(MessageBeingWritten&);
  void writeMessage_(MessageBeingWritten&);
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
    TP_VLOG() << "Pipe " << id_ << " is writing proto (spontaneous connection)";
    connection_->write(
        *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_
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
    TP_VLOG() << "Pipe " << id_ << " is writing proto (brochure)";
    connection_->write(
        *pbPacketOut2, lazyCallbackWrapper_([pbPacketOut2](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_ << " done writing proto (brochure)";
        }));
    state_ = CLIENT_WAITING_FOR_BROCHURE_ANSWER;
    auto pbPacketIn = std::make_shared<proto::Packet>();
    TP_VLOG() << "Pipe " << id_ << " is reading proto (brochure answer)";
    connection_->read(
        *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_
                    << " done reading proto (brochure answer)";
          impl.onReadWhileClientWaitingForBrochureAnswer_(*pbPacketIn);
        }));
  }
  if (state_ == SERVER_WAITING_FOR_BROCHURE) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    TP_VLOG() << "Pipe " << id_ << " is reading proto (brochure)";
    connection_->read(
        *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_ << " done reading proto (brochure)";
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
  TP_VLOG() << "Pipe " << id_ << " is closing";
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
  TP_VLOG() << "Pipe " << id_ << " received a readDescriptor request (#"
            << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextReadDescriptorCallbackToCall_++);
    TP_VLOG() << "Pipe " << id_ << " is calling a readDescriptor callback (#"
              << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG() << "Pipe " << id_ << " done calling a readDescriptor callback (#"
              << sequenceNumber << ")";
  };

  messagesBeingExpected_.push_back(
      MessageBeingExpected{sequenceNumber, std::move(fn)});

  if (error_) {
    triggerReadyCallbacks_();
    return;
  }

  if (messagesBeingExpected_.size() == 1 && state_ == ESTABLISHED &&
      connectionState_ == NEXT_UP_IS_DESCRIPTOR) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    TP_VLOG() << "Pipe " << id_ << " is reading proto (message descriptor)";
    connection_->read(
        *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_
                    << " done reading proto (message descriptor)";
          impl.onReadOfMessageDescriptor_(*pbPacketIn);
        }));
    connectionState_ = NEXT_UP_IS_PAYLOADS;
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
  TP_THROW_ASSERT_IF(messagesBeingAllocated_.empty());

  MessageBeingAllocated messageBeingAllocated{
      std::move(messagesBeingAllocated_.front())};
  messagesBeingAllocated_.pop_front();

  // Other sanity checks that must pass unless the user really messed up.
  size_t numPayloads = message.payloads.size();
  TP_THROW_ASSERT_IF(numPayloads != messageBeingAllocated.payloads.size());
  for (size_t payloadIdx = 0; payloadIdx < numPayloads; payloadIdx++) {
    Message::Payload& payload = message.payloads[payloadIdx];
    MessageBeingAllocated::Payload& payloadBeingAllocated =
        messageBeingAllocated.payloads[payloadIdx];
    TP_DCHECK_GE(payloadBeingAllocated.length, 0);
    TP_THROW_ASSERT_IF(payload.length != payloadBeingAllocated.length);
  }
  size_t numTensors = message.tensors.size();
  TP_THROW_ASSERT_IF(numTensors != messageBeingAllocated.tensors.size());
  for (size_t tensorIdx = 0; tensorIdx < numTensors; tensorIdx++) {
    Message::Tensor& tensor = message.tensors[tensorIdx];
    MessageBeingAllocated::Tensor& tensorBeingAllocated =
        messageBeingAllocated.tensors[tensorIdx];
    TP_DCHECK_GE(tensorBeingAllocated.length, 0);
    TP_THROW_ASSERT_IF(tensor.length != tensorBeingAllocated.length);
  }

  int64_t sequenceNumber = messageBeingAllocated.sequenceNumber;
  TP_VLOG() << "Pipe " << id_ << " received a read request (#" << sequenceNumber
            << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextReadCallbackToCall_++);
    TP_VLOG() << "Pipe " << id_ << " is calling a read callback (#"
              << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG() << "Pipe " << id_ << " done calling a read callback (#"
              << sequenceNumber << ")";
  };

  MessageBeingRead messageBeingRead{
      sequenceNumber, std::move(message), std::move(fn)};

  if (error_) {
    messagesBeingRead_.push_back(std::move(messageBeingRead));
    triggerReadyCallbacks_();
    return;
  }

  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_PAYLOADS);
  for (size_t payloadIdx = 0; payloadIdx < numPayloads; payloadIdx++) {
    Message::Payload& payload = messageBeingRead.message.payloads[payloadIdx];
    TP_VLOG() << "Pipe " << id_ << " is reading payload";
    connection_->read(
        payload.data,
        payload.length,
        eagerCallbackWrapper_(
            [sequenceNumber](
                Impl& impl, const void* /* unused */, size_t /* unused */) {
              TP_VLOG() << "Pipe " << impl.id_ << " done reading payload";
              impl.onReadOfPayload_(sequenceNumber);
            }));
    ++messageBeingRead.numPayloadsStillBeingRead;
  }
  connectionState_ = NEXT_UP_IS_DESCRIPTOR;

  for (size_t tensorIdx = 0; tensorIdx < numTensors; tensorIdx++) {
    Message::Tensor& tensor = messageBeingRead.message.tensors[tensorIdx];
    MessageBeingAllocated::Tensor& tensorBeingAllocated =
        messageBeingAllocated.tensors[tensorIdx];
    std::shared_ptr<channel::Channel> channel =
        channels_.at(tensorBeingAllocated.channelName);
    TP_VLOG() << "Pipe " << id_ << " is receiving tensor";
    channel->recv(
        std::move(tensorBeingAllocated.descriptor),
        tensor.data,
        tensor.length,
        eagerCallbackWrapper_([sequenceNumber](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_ << " done receiving tensor";
          impl.onRecvOfTensor_(sequenceNumber);
        }));
    ++messageBeingRead.numTensorsStillBeingReceived;
  }

  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
  if (!messagesBeingExpected_.empty()) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    TP_VLOG() << "Pipe " << id_ << " is reading proto (message descriptor)";
    connection_->read(
        *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_
                    << " done reading proto (message descriptor)";
          impl.onReadOfMessageDescriptor_(*pbPacketIn);
        }));
    connectionState_ = NEXT_UP_IS_PAYLOADS;
  }

  messagesBeingRead_.push_back(std::move(messageBeingRead));
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
  TP_VLOG() << "Pipe " << id_ << " received a write request (#"
            << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, Message message) {
    TP_DCHECK_EQ(sequenceNumber, nextWriteCallbackToCall_++);
    TP_VLOG() << "Pipe " << id_ << " is calling a write callback (#"
              << sequenceNumber << ")";
    fn(error, std::move(message));
    TP_VLOG() << "Pipe " << id_ << " done calling a write callback (#"
              << sequenceNumber << ")";
  };

  messagesBeingWritten_.push_back(
      MessageBeingWritten{sequenceNumber, std::move(message), std::move(fn)});

  if (error_) {
    triggerReadyCallbacks_();
    return;
  }

  if (state_ == ESTABLISHED) {
    sendTensorsOfMessage_(messagesBeingWritten_.back());
  }
}

//
// Helpers to schedule our callbacks into user code
//

void Pipe::Impl::triggerReadyCallbacks_() {
  TP_DCHECK(loop_.inLoop());

  while (true) {
    if (!messagesBeingExpected_.empty()) {
      if (error_) {
        MessageBeingExpected mVal = std::move(messagesBeingExpected_.front());
        messagesBeingExpected_.pop_front();
        mVal.callback(error_, Message());
        continue;
      }
    }
    if (!messagesBeingRead_.empty()) {
      MessageBeingRead& mRef = messagesBeingRead_.front();
      // We don't check for error, because even in case of error we still must
      // wait for all transport/channel callbacks to return to be sure that no
      // other thread is working on the data.
      if (mRef.numPayloadsStillBeingRead == 0 &&
          mRef.numTensorsStillBeingReceived == 0) {
        MessageBeingRead mVal = std::move(messagesBeingRead_.front());
        messagesBeingRead_.pop_front();
        mVal.callback(error_, std::move(mVal.message));
        continue;
      }
    }
    if (!messagesBeingWritten_.empty()) {
      MessageBeingWritten& mRef = messagesBeingWritten_.front();
      // If the message has already been written we don't check for error,
      // because even in case of error we still must wait for all transport/
      // channel callbacks to return to be sure that no other thread is working
      // on the data.
      bool doneWritingData =
          mRef.startedWritingPayloads && mRef.numPayloadsStillBeingWritten == 0;
      bool blockedWritingData =
          mRef.startedWritingPayloads && mRef.numPayloadsStillBeingWritten > 0;
      bool doneSendingTensors = mRef.startedSendingTensors &&
          mRef.numTensorDescriptorsStillBeingCollected == 0 &&
          mRef.numTensorsStillBeingSent == 0;
      bool blockedWritingTensors = mRef.startedSendingTensors &&
          (mRef.numTensorDescriptorsStillBeingCollected > 0 ||
           mRef.numTensorsStillBeingSent > 0);
      if ((error_ && !blockedWritingData && !blockedWritingTensors) ||
          (doneWritingData && doneSendingTensors)) {
        MessageBeingWritten mVal = std::move(messagesBeingWritten_.front());
        messagesBeingWritten_.pop_front();
        mVal.callback(error_, std::move(mVal.message));
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
  TP_VLOG() << "Pipe " << id_ << " is handling error " << error_.what();

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

void Pipe::Impl::doWritesAccumulatedWhileWaitingForPipeToBeEstablished_() {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  for (auto& m : messagesBeingWritten_) {
    if (m.startedSendingTensors) {
      break;
    }
    sendTensorsOfMessage_(m);
  }
}

void Pipe::Impl::sendTensorsOfMessage_(
    MessageBeingWritten& messageBeingWritten) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!messageBeingWritten.startedSendingTensors);
  messageBeingWritten.startedSendingTensors = true;

  if (messageBeingWritten.message.tensors.size() == 0) {
    checkForMessagesDoneCollectingTensorDescriptors_();
    return;
  }

  for (int tensorIdx = 0;
       tensorIdx < messageBeingWritten.message.tensors.size();
       ++tensorIdx) {
    const auto& tensor = messageBeingWritten.message.tensors[tensorIdx];
    bool foundAChannel = false;
    for (const auto& channelContextIter : context_->getOrderedChannels()) {
      const std::string& channelName = std::get<0>(channelContextIter.second);

      auto channelIter = channels_.find(channelName);
      if (channelIter == channels_.cend()) {
        continue;
      }
      channel::Channel& channel = *(channelIter->second);

      TP_VLOG() << "Pipe " << id_ << " is sending tensor";
      channel.send(
          tensor.data,
          tensor.length,
          eagerCallbackWrapper_(
              [sequenceNumber{messageBeingWritten.sequenceNumber}, tensorIdx](
                  Impl& impl, channel::Channel::TDescriptor descriptor) {
                TP_VLOG() << "Pipe " << impl.id_ << " got tensor descriptor";
                impl.onDescriptorOfTensor_(
                    sequenceNumber, tensorIdx, std::move(descriptor));
              }),
          eagerCallbackWrapper_(
              [sequenceNumber{messageBeingWritten.sequenceNumber}](Impl& impl) {
                TP_VLOG() << "Pipe " << impl.id_ << " done sending tensor";
                impl.onSendOfTensor_(sequenceNumber);
              }));
      messageBeingWritten.tensors.push_back(
          MessageBeingWritten::Tensor{channelName});
      ++messageBeingWritten.numTensorDescriptorsStillBeingCollected;
      ++messageBeingWritten.numTensorsStillBeingSent;

      foundAChannel = true;
      break;
    }
    TP_THROW_ASSERT_IF(!foundAChannel);
  }
}

void Pipe::Impl::writeMessage_(MessageBeingWritten& messageBeingWritten) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!messageBeingWritten.startedWritingPayloads);
  messageBeingWritten.startedWritingPayloads = true;

  auto pbPacketOut = std::make_shared<proto::Packet>();
  proto::MessageDescriptor* pbMessageDescriptor =
      pbPacketOut->mutable_message_descriptor();

  pbMessageDescriptor->set_metadata(messageBeingWritten.message.metadata);

  for (int payloadIdx = 0;
       payloadIdx < messageBeingWritten.message.payloads.size();
       ++payloadIdx) {
    const auto& payload = messageBeingWritten.message.payloads[payloadIdx];
    proto::MessageDescriptor::PayloadDescriptor* pbPayloadDescriptor =
        pbMessageDescriptor->add_payload_descriptors();
    pbPayloadDescriptor->set_size_in_bytes(payload.length);
    pbPayloadDescriptor->set_metadata(payload.metadata);
  }

  TP_DCHECK_EQ(
      messageBeingWritten.message.tensors.size(),
      messageBeingWritten.tensors.size());
  for (int tensorIdx = 0; tensorIdx < messageBeingWritten.tensors.size();
       ++tensorIdx) {
    const auto& tensor = messageBeingWritten.message.tensors[tensorIdx];
    const auto& otherTensor = messageBeingWritten.tensors[tensorIdx];
    proto::MessageDescriptor::TensorDescriptor* pbTensorDescriptor =
        pbMessageDescriptor->add_tensor_descriptors();
    pbTensorDescriptor->set_device_type(proto::DeviceType::DEVICE_TYPE_CPU);
    pbTensorDescriptor->set_size_in_bytes(tensor.length);
    pbTensorDescriptor->set_metadata(tensor.metadata);
    pbTensorDescriptor->set_channel_name(otherTensor.channelName);
    // FIXME In principle we could move here.
    pbTensorDescriptor->set_channel_descriptor(otherTensor.descriptor);
  }

  TP_VLOG() << "Pipe " << id_ << " is writing proto (message descriptor)";
  connection_->write(
      *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
        TP_VLOG() << "Pipe " << impl.id_
                  << " done writing proto (message descriptor)";
      }));

  for (size_t payloadIdx = 0;
       payloadIdx < messageBeingWritten.message.payloads.size();
       payloadIdx++) {
    Message::Payload& payload =
        messageBeingWritten.message.payloads[payloadIdx];
    TP_VLOG() << "Pipe " << id_ << " is writing payload";
    connection_->write(
        payload.data,
        payload.length,
        eagerCallbackWrapper_(
            [sequenceNumber{messageBeingWritten.sequenceNumber}](Impl& impl) {
              TP_VLOG() << "Pipe " << impl.id_ << " done writing payload";
              impl.onWriteOfPayload_(sequenceNumber);
            }));
    ++messageBeingWritten.numPayloadsStillBeingWritten;
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
      TP_VLOG() << "Pipe " << id_
                << " is requesting connection (as replacement)";
      uint64_t token =
          listener_->registerConnectionRequest(lazyCallbackWrapper_(
              [](Impl& impl,
                 std::string transport,
                 std::shared_ptr<transport::Connection> connection) {
                TP_VLOG() << "Pipe " << impl.id_
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

    TP_VLOG() << "Pipe " << id_ << " is requesting connection (for channel)";
    uint64_t token = listener_->registerConnectionRequest(lazyCallbackWrapper_(
        [channelName](
            Impl& impl,
            std::string transport,
            std::shared_ptr<transport::Connection> connection) {
          TP_VLOG() << "Pipe " << impl.id_
                    << " done requesting connection (for channel)";
          impl.onAcceptWhileServerWaitingForChannel_(
              channelName, std::move(transport), std::move(connection));
        }));
    channelRegistrationIds_[channelName] = token;
    needToWaitForConnections = true;
    proto::ChannelSelection* pbChannelSelection =
        &(*pbAllChannelSelections)[channelName];
    pbChannelSelection->set_registration_id(token);
  }

  TP_VLOG() << "Pipe " << id_ << " is writing proto (brochure answer)";
  connection_->write(
      *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
        TP_VLOG() << "Pipe " << impl.id_
                  << " done writing proto (brochure answer)";
      }));

  if (!needToWaitForConnections) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      TP_VLOG() << "Pipe " << id_ << " is reading proto (message descriptor)";
      connection_->read(
          *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
            TP_VLOG() << "Pipe " << impl.id_
                      << " done reading proto (message descriptor)";
            impl.onReadOfMessageDescriptor_(*pbPacketIn);
          }));
      connectionState_ = NEXT_UP_IS_PAYLOADS;
    }
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
    TP_VLOG() << "Pipe " << id_ << " is opening connection (as replacement)";
    std::shared_ptr<transport::Connection> connection =
        transportContext->connect(address);
    connection->setId(id_ + ".tr_" + transport);
    auto pbPacketOut = std::make_shared<proto::Packet>();
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut->mutable_requested_connection();
    uint64_t token = pbBrochureAnswer.registration_id();
    pbRequestedConnection->set_registration_id(token);
    TP_VLOG() << "Pipe " << id_ << " is writing proto (requested connection)";
    connection->write(
        *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_
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

    TP_VLOG() << "Pipe " << id_ << " is opening connection (for channel)";
    std::shared_ptr<transport::Connection> connection =
        transportContext->connect(address);
    connection->setId(id_ + ".ch_" + channelName);

    auto pbPacketOut = std::make_shared<proto::Packet>();
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut->mutable_requested_connection();
    uint64_t token = pbChannelSelection.registration_id();
    pbRequestedConnection->set_registration_id(token);
    TP_VLOG() << "Pipe " << id_ << " is writing proto (requested connection)";
    connection->write(
        *pbPacketOut, lazyCallbackWrapper_([pbPacketOut](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_
                    << " done writing proto (requested connection)";
        }));

    std::shared_ptr<channel::Channel> channel = channelContext->createChannel(
        std::move(connection), channel::Channel::Endpoint::kConnect);
    channel->setId(id_ + ".ch_" + channelName);
    channels_.emplace(channelName, std::move(channel));
  }

  state_ = ESTABLISHED;
  doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
  if (!messagesBeingExpected_.empty()) {
    auto pbPacketIn2 = std::make_shared<proto::Packet>();
    TP_VLOG() << "Pipe " << id_ << " is reading proto (message descriptor)";
    connection_->read(
        *pbPacketIn2, lazyCallbackWrapper_([pbPacketIn2](Impl& impl) {
          TP_VLOG() << "Pipe " << impl.id_
                    << " done reading proto (message descriptor)";
          impl.onReadOfMessageDescriptor_(*pbPacketIn2);
        }));
    connectionState_ = NEXT_UP_IS_PAYLOADS;
  }
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
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      TP_VLOG() << "Pipe " << id_ << " is reading proto (message descriptor)";
      connection_->read(
          *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
            TP_VLOG() << "Pipe " << impl.id_
                      << " done reading proto (message descriptor)";
            impl.onReadOfMessageDescriptor_(*pbPacketIn);
          }));
      connectionState_ = NEXT_UP_IS_PAYLOADS;
    }
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
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      TP_VLOG() << "Pipe " << id_ << " is reading proto (message descriptor)";
      connection_->read(
          *pbPacketIn, lazyCallbackWrapper_([pbPacketIn](Impl& impl) {
            TP_VLOG() << "Pipe " << impl.id_
                      << " done reading proto (message descriptor)";
            impl.onReadOfMessageDescriptor_(*pbPacketIn);
          }));
      connectionState_ = NEXT_UP_IS_PAYLOADS;
    }
  }
}

void Pipe::Impl::onReadOfMessageDescriptor_(const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  TP_DCHECK(!messagesBeingExpected_.empty());
  MessageBeingExpected messageBeingExpected =
      std::move(messagesBeingExpected_.front());
  messagesBeingExpected_.pop_front();

  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kMessageDescriptor);
  const proto::MessageDescriptor& pbMessageDescriptor =
      pbPacketIn.message_descriptor();

  Message message;
  MessageBeingAllocated messageBeingAllocated;

  messageBeingAllocated.sequenceNumber = messageBeingExpected.sequenceNumber;
  message.metadata = pbMessageDescriptor.metadata();
  for (const auto& pbPayloadDescriptor :
       pbMessageDescriptor.payload_descriptors()) {
    Message::Payload payload;
    MessageBeingAllocated::Payload payloadBeingAllocated;
    payload.length = pbPayloadDescriptor.size_in_bytes();
    payloadBeingAllocated.length = payload.length;
    payload.metadata = pbPayloadDescriptor.metadata();
    message.payloads.push_back(std::move(payload));
    messageBeingAllocated.payloads.push_back(std::move(payloadBeingAllocated));
  }
  for (const auto& pbTensorDescriptor :
       pbMessageDescriptor.tensor_descriptors()) {
    Message::Tensor tensor;
    MessageBeingAllocated::Tensor tensorBeingAllocated;
    tensor.length = pbTensorDescriptor.size_in_bytes();
    tensorBeingAllocated.length = tensor.length;
    tensor.metadata = pbTensorDescriptor.metadata();
    tensorBeingAllocated.channelName = pbTensorDescriptor.channel_name();
    // FIXME If the protobuf wasn't const we could move the string out...
    tensorBeingAllocated.descriptor = pbTensorDescriptor.channel_descriptor();
    message.tensors.push_back(std::move(tensor));
    messageBeingAllocated.tensors.push_back(std::move(tensorBeingAllocated));
  }

  messagesBeingAllocated_.push_back(std::move(messageBeingAllocated));

  messageBeingExpected.callback(Error::kSuccess, std::move(message));
}

void Pipe::Impl::onDescriptorOfTensor_(
    int64_t sequenceNumber,
    int64_t tensorIdx,
    channel::Channel::TDescriptor descriptor) {
  TP_DCHECK(loop_.inLoop());
  // FIXME Using find_if makes sense when we expect the result to be near the
  // beginning, but here it's actually likely to be at the back. Perhaps it
  // makes sense to just use a hashmap?
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  TP_DCHECK(messageBeingWritten.startedSendingTensors);
  TP_DCHECK_LT(tensorIdx, messageBeingWritten.tensors.size());
  messageBeingWritten.tensors[tensorIdx].descriptor = std::move(descriptor);
  --messageBeingWritten.numTensorDescriptorsStillBeingCollected;
  checkForMessagesDoneCollectingTensorDescriptors_();
}

void Pipe::Impl::onReadOfPayload_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.numPayloadsStillBeingRead--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onRecvOfTensor_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.numTensorsStillBeingReceived--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onWriteOfPayload_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.numPayloadsStillBeingWritten--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onSendOfTensor_(int64_t sequenceNumber) {
  TP_DCHECK(loop_.inLoop());
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.numTensorsStillBeingSent--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::checkForMessagesDoneCollectingTensorDescriptors_() {
  // FIXME It's not great to iterate over all pending messages each time. If
  // this was a map indexed by sequence number we could just store the sequence
  // number of the last message we had written and resume looking from there.
  for (MessageBeingWritten& mRef : messagesBeingWritten_) {
    if (mRef.startedWritingPayloads) {
      continue;
    }
    if (mRef.numTensorDescriptorsStillBeingCollected == 0) {
      writeMessage_(mRef);
    } else {
      break;
    }
  }
}

} // namespace tensorpipe
