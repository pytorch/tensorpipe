/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/pipe.h>

#include <algorithm>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/proto/core.pb.h>

namespace tensorpipe {

//
// Initialization
//

std::shared_ptr<Pipe> Pipe::create(
    std::shared_ptr<Context> context,
    const std::string& url) {
  return std::make_shared<Pipe>(ConstructorToken(), std::move(context), url);
}

std::shared_ptr<Pipe::Impl> Pipe::Impl::create(
    std::shared_ptr<Context> context,
    const std::string& url) {
  std::string transport;
  std::string address;
  std::tie(transport, address) = splitSchemeOfURL(url);
  std::shared_ptr<transport::Connection> connection =
      context->getContextForTransport_(transport)->connect(std::move(address));
  auto impl = std::make_shared<Impl>(
      ConstructorToken(),
      std::move(context),
      std::move(transport),
      std::move(connection));
  impl->start_();
  return impl;
}

std::shared_ptr<Pipe::Impl> Pipe::Impl::create(
    std::shared_ptr<Context> context,
    std::shared_ptr<Listener> listener,
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  auto impl = std::make_shared<Impl>(
      ConstructorToken(),
      std::move(context),
      std::move(listener),
      std::move(transport),
      std::move(connection));
  impl->start_();
  return impl;
}

Pipe::Pipe(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    const std::string& url)
    : impl_(Impl::create(std::move(context), url)) {}

Pipe::Pipe(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    std::shared_ptr<Listener> listener,
    std::string transport,
    std::shared_ptr<transport::Connection> connection)
    : impl_(Impl::create(
          std::move(context),
          std::move(listener),
          std::move(transport),
          std::move(connection))) {}

Pipe::Impl::Impl(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    std::string transport,
    std::shared_ptr<transport::Connection> connection)
    : state_(CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE),
      context_(std::move(context)),
      transport_(std::move(transport)),
      connection_(std::move(connection)),
      readCallbackWrapper_(*this),
      readPacketCallbackWrapper_(*this),
      writeCallbackWrapper_(*this),
      writePacketCallbackWrapper_(*this),
      connectionRequestCallbackWrapper_(*this),
      channelRecvCallbackWrapper_(*this),
      channelSendCallbackWrapper_(*this) {}

Pipe::Impl::Impl(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    std::shared_ptr<Listener> listener,
    std::string transport,
    std::shared_ptr<transport::Connection> connection)
    : state_(SERVER_WAITING_FOR_BROCHURE),
      context_(std::move(context)),
      listener_(std::move(listener)),
      transport_(std::move(transport)),
      connection_(std::move(connection)),
      readCallbackWrapper_(*this),
      readPacketCallbackWrapper_(*this),
      writeCallbackWrapper_(*this),
      writePacketCallbackWrapper_(*this),
      connectionRequestCallbackWrapper_(*this),
      channelRecvCallbackWrapper_(*this),
      channelSendCallbackWrapper_(*this) {}

void Pipe::Impl::start_() {
  deferToLoop_([this]() { startFromLoop_(); });
}

void Pipe::Impl::startFromLoop_() {
  TP_DCHECK(inLoop_());
  if (state_ == CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE) {
    auto pbPacketOut = std::make_shared<proto::Packet>();
    // This makes the packet contain a SpontaneousConnection message.
    pbPacketOut->mutable_spontaneous_connection();
    connection_->write(
        *pbPacketOut,
        writePacketCallbackWrapper_([pbPacketOut](Impl& /* unused */) {}));

    auto pbPacketOut2 = std::make_shared<proto::Packet>();
    proto::Brochure* pbBrochure = pbPacketOut2->mutable_brochure();
    auto pbAllTransportAdvertisements =
        pbBrochure->mutable_transport_advertisement();
    for (const auto& contextIter : context_->contexts_) {
      const std::string& transport = contextIter.first;
      const transport::Context& context = *(contextIter.second);
      proto::TransportAdvertisement* pbTransportAdvertisement =
          &(*pbAllTransportAdvertisements)[transport];
      pbTransportAdvertisement->set_domain_descriptor(
          context.domainDescriptor());
    }
    auto pbAllChannelAdvertisements =
        pbBrochure->mutable_channel_advertisement();
    for (const auto& channelFactoryIter : context_->channelFactories_) {
      const std::string& name = channelFactoryIter.first;
      const channel::ChannelFactory& channelFactory =
          *(channelFactoryIter.second);
      proto::ChannelAdvertisement* pbChannelAdvertisement =
          &(*pbAllChannelAdvertisements)[name];
      pbChannelAdvertisement->set_domain_descriptor(
          channelFactory.domainDescriptor());
    }
    connection_->write(
        *pbPacketOut2,
        writePacketCallbackWrapper_([pbPacketOut2](Impl& /* unused */) {}));
    state_ = CLIENT_WAITING_FOR_BROCHURE_ANSWER;
    auto pbPacketIn = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
          impl.onReadWhileClientWaitingForBrochureAnswer_(*pbPacketIn);
        }));
  }
  if (state_ == SERVER_WAITING_FOR_BROCHURE) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
          impl.onReadWhileServerWaitingForBrochure_(*pbPacketIn);
        }));
  }
}

Pipe::~Pipe() {
  impl_->close();
}

void Pipe::Impl::close() {
  deferToLoop_([this]() { closeFromLoop_(); });
}

void Pipe::Impl::closeFromLoop_() {
  TP_DCHECK(inLoop_());

  // TODO Make a RAII wrapper so that this isn't necessary.
  if (registrationId_.has_value()) {
    listener_->unregisterConnectionRequest_(registrationId_.value());
    registrationId_.reset();
  }
  for (const auto& iter : channelRegistrationIds_) {
    listener_->unregisterConnectionRequest_(iter.second);
  }
  channelRegistrationIds_.clear();
  error_ = TP_CREATE_ERROR(PipeClosedError);
  handleError_();
}

bool Pipe::Impl::inLoop_() {
  // If the current thread is already holding the lock (i.e., it's already in
  // this function somewhere higher up in the stack) then this check won't race
  // and we will detect it correctly. If this is not the case, then this check
  // may race with another thread, but that's nothing to worry about because in
  // either case the outcome will be negative.
  return currentLoop_ == std::this_thread::get_id();
}

void Pipe::Impl::deferToLoop_(std::function<void()> fn) {
  if (inLoop_()) {
    pendingTasks_.push_back(std::move(fn));
    return;
  }

  std::unique_lock<std::mutex> lock(mutex_);
  currentLoop_ = std::this_thread::get_id();

  fn();

  while (!pendingTasks_.empty()) {
    pendingTasks_.front()();
    pendingTasks_.pop_front();
  }

  // FIXME Use some RAII pattern to make sure this is reset in case of exception
  currentLoop_ = std::thread::id();
}

//
// Entry points for user code
//

void Pipe::readDescriptor(read_descriptor_callback_fn fn) {
  impl_->readDescriptor(std::move(fn));
}

void Pipe::Impl::readDescriptor(read_descriptor_callback_fn fn) {
  deferToLoop_([this, fn{std::move(fn)}]() mutable {
    readDescriptorFromLoop_(std::move(fn));
  });
}

void Pipe::Impl::readDescriptorFromLoop_(read_descriptor_callback_fn fn) {
  TP_DCHECK(inLoop_());

  int64_t sequenceNumber = nextMessageBeingRead_++;

  if (error_) {
    triggerReadDescriptorCallback_(
        sequenceNumber, std::move(fn), error_, Message());
    return;
  }

  messagesBeingExpected_.push_back(
      MessageBeingExpected{sequenceNumber, std::move(fn)});

  if (messagesBeingExpected_.size() == 1 && state_ == ESTABLISHED &&
      connectionState_ == NEXT_UP_IS_DESCRIPTOR) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
          impl.onReadOfMessageDescriptor_(*pbPacketIn);
        }));
    connectionState_ = NEXT_UP_IS_DATA;
  }
}

void Pipe::read(Message message, read_callback_fn fn) {
  impl_->read(std::move(message), std::move(fn));
}

void Pipe::Impl::read(Message message, read_callback_fn fn) {
  // Messages aren't copyable and thus if a lambda captures them it cannot be
  // wrapped in a std::function. Therefore we wrap Messages in shared_ptrs.
  auto sharedMessage = std::make_shared<Message>(std::move(message));
  deferToLoop_([this,
                sharedMessage{std::move(sharedMessage)},
                fn{std::move(fn)}]() mutable {
    readFromLoop_(std::move(*sharedMessage), std::move(fn));
  });
}

void Pipe::Impl::readFromLoop_(Message message, read_callback_fn fn) {
  TP_DCHECK(inLoop_());

  // This is such a bad logical error on the user's side that it doesn't deserve
  // to pass through the channel for "expected errors" (i.e., the callback).
  TP_THROW_ASSERT_IF(messagesBeingAllocated_.empty());

  MessageBeingAllocated messageBeingAllocated{
      std::move(messagesBeingAllocated_.front())};
  messagesBeingAllocated_.pop_front();

  int64_t sequenceNumber = messageBeingAllocated.sequenceNumber;

  if (error_) {
    triggerReadCallback_(
        sequenceNumber, std::move(fn), error_, std::move(message));
    return;
  }

  MessageBeingRead messageBeingRead;

  messageBeingRead.sequenceNumber = sequenceNumber;
  TP_DCHECK_GE(messageBeingAllocated.length, 0);
  TP_THROW_ASSERT_IF(message.length != messageBeingAllocated.length);
  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DATA);
  connection_->read(
      message.data,
      message.length,
      readCallbackWrapper_(
          [sequenceNumber](
              Impl& impl, const void* /* unused */, size_t /* unused */) {
            impl.onReadOfMessageData_(sequenceNumber);
          }));
  connectionState_ = NEXT_UP_IS_DESCRIPTOR;

  size_t numTensors = message.tensors.size();
  TP_THROW_ASSERT_IF(numTensors != messageBeingAllocated.tensors.size());
  for (size_t tensorIdx = 0; tensorIdx < numTensors; tensorIdx++) {
    Message::Tensor& tensor = message.tensors[tensorIdx];
    MessageBeingAllocated::Tensor& tensorBeingAllocated =
        messageBeingAllocated.tensors[tensorIdx];
    TP_DCHECK_GE(tensorBeingAllocated.length, 0);
    TP_THROW_ASSERT_IF(tensor.length != tensorBeingAllocated.length);
    std::shared_ptr<channel::Channel> channel =
        channels_.at(tensorBeingAllocated.channelName);
    channel->recv(
        std::move(tensorBeingAllocated.channelDescriptor),
        tensor.data,
        tensor.length,
        channelRecvCallbackWrapper_([sequenceNumber](Impl& impl) {
          impl.onRecvOfTensorData_(sequenceNumber);
        }));
    messageBeingRead.numTensorDataStillBeingReceived++;
  }

  messageBeingRead.message = std::move(message);
  messageBeingRead.callback = std::move(fn);

  messagesBeingRead_.push_back(std::move(messageBeingRead));

  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
  if (!messagesBeingExpected_.empty()) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
          impl.onReadOfMessageDescriptor_(*pbPacketIn);
        }));
    connectionState_ = NEXT_UP_IS_DATA;
  }
}

void Pipe::write(Message message, write_callback_fn fn) {
  impl_->write(std::move(message), std::move(fn));
}

void Pipe::Impl::write(Message message, write_callback_fn fn) {
  // Messages aren't copyable and thus if a lambda captures them it cannot be
  // wrapped in a std::function. Therefore we wrap Messages in shared_ptrs.
  auto sharedMessage = std::make_shared<Message>(std::move(message));
  deferToLoop_([this,
                sharedMessage{std::move(sharedMessage)},
                fn{std::move(fn)}]() mutable {
    writeFromLoop_(std::move(*sharedMessage), std::move(fn));
  });
}

void Pipe::Impl::writeFromLoop_(Message message, write_callback_fn fn) {
  TP_DCHECK(inLoop_());

  int64_t sequenceNumber = nextMessageBeingWritten_++;

  if (error_) {
    triggerWriteCallback_(
        sequenceNumber, std::move(fn), error_, std::move(message));
    return;
  }

  if (state_ == ESTABLISHED) {
    writeWhenEstablished_(sequenceNumber, std::move(message), std::move(fn));
  } else {
    messagesBeingQueued_.push_back(
        MessageBeingQueued{sequenceNumber, std::move(message), std::move(fn)});
  }
}

//
// Helpers to schedule our callbacks into user code
//

void Pipe::Impl::triggerReadDescriptorCallback_(
    int64_t sequenceNumber,
    read_descriptor_callback_fn&& fn,
    const Error& error,
    Message message) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(nextReadDescriptorCallbackToCall_, sequenceNumber);
  fn(error, std::move(message));
  nextReadDescriptorCallbackToCall_++;
}

void Pipe::Impl::triggerReadCallback_(
    int64_t sequenceNumber,
    read_callback_fn&& fn,
    const Error& error,
    Message message) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(nextReadCallbackToCall_, sequenceNumber);
  fn(error, std::move(message));
  nextReadCallbackToCall_++;
}

void Pipe::Impl::triggerWriteCallback_(
    int64_t sequenceNumber,
    write_callback_fn&& fn,
    const Error& error,
    Message message) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(nextWriteCallbackToCall_, sequenceNumber);
  fn(error, std::move(message));
  nextWriteCallbackToCall_++;
}

void Pipe::Impl::triggerReadyCallbacks_() {
  TP_DCHECK(inLoop_());

  while (true) {
    if (!messagesBeingExpected_.empty()) {
      if (error_) {
        MessageBeingExpected mVal = std::move(messagesBeingExpected_.front());
        messagesBeingExpected_.pop_front();
        triggerReadDescriptorCallback_(
            mVal.sequenceNumber, std::move(mVal.callback), error_, Message());
        continue;
      }
    }
    if (!messagesBeingRead_.empty()) {
      MessageBeingRead& mRef = messagesBeingRead_.front();
      if (error_ ||
          (!mRef.dataStillBeingRead &&
           mRef.numTensorDataStillBeingReceived == 0)) {
        MessageBeingRead mVal = std::move(messagesBeingRead_.front());
        messagesBeingRead_.pop_front();
        triggerReadCallback_(
            mVal.sequenceNumber,
            std::move(mVal.callback),
            error_,
            std::move(mVal.message));
        continue;
      }
    }
    if (!messagesBeingWritten_.empty()) {
      MessageBeingWritten& mRef = messagesBeingWritten_.front();
      if (error_ ||
          (!mRef.dataStillBeingWritten &&
           mRef.numTensorDataStillBeingSent == 0)) {
        MessageBeingWritten mVal = std::move(messagesBeingWritten_.front());
        messagesBeingWritten_.pop_front();
        triggerWriteCallback_(
            mVal.sequenceNumber,
            std::move(mVal.callback),
            error_,
            std::move(mVal.message));
        continue;
      }
    }
    if (!messagesBeingQueued_.empty()) {
      if (error_) {
        MessageBeingQueued mVal = std::move(messagesBeingQueued_.front());
        messagesBeingQueued_.pop_front();
        triggerWriteCallback_(
            mVal.sequenceNumber,
            std::move(mVal.callback),
            error_,
            std::move(mVal.message));
        continue;
      }
    }
    break;
  }
}

//
// Error handling
//

void Pipe::Impl::handleError_() {
  TP_DCHECK(inLoop_());
  triggerReadyCallbacks_();
}

//
// Everything else
//

void Pipe::Impl::doWritesAccumulatedWhileWaitingForPipeToBeEstablished_() {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  while (!messagesBeingQueued_.empty()) {
    MessageBeingQueued m = std::move(messagesBeingQueued_.front());
    messagesBeingQueued_.pop_front();
    writeWhenEstablished_(
        m.sequenceNumber, std::move(m.message), std::move(m.callback));
  }
}

void Pipe::Impl::writeWhenEstablished_(
    int64_t sequenceNumber,
    Message message,
    write_callback_fn fn) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  MessageBeingWritten messageBeingWritten;
  auto pbPacketOut = std::make_shared<proto::Packet>();
  proto::MessageDescriptor* pbMessageDescriptor =
      pbPacketOut->mutable_message_descriptor();

  messageBeingWritten.sequenceNumber = sequenceNumber;
  pbMessageDescriptor->set_size_in_bytes(message.length);
  pbMessageDescriptor->set_metadata(message.metadata);

  for (const auto& tensor : message.tensors) {
    proto::MessageDescriptor::TensorDescriptor* pbTensorDescriptor =
        pbMessageDescriptor->add_tensor_descriptors();
    pbTensorDescriptor->set_device_type(proto::DeviceType::DEVICE_TYPE_CPU);
    pbTensorDescriptor->set_size_in_bytes(tensor.length);
    pbTensorDescriptor->set_metadata(tensor.metadata);

    bool foundAChannel = false;
    for (const auto& channelFactoryIter :
         context_->channelFactoriesByPriority_) {
      const std::string& name = std::get<0>(channelFactoryIter.second);

      auto channelIter = channels_.find(name);
      if (channelIter == channels_.cend()) {
        continue;
      }
      channel::Channel& channel = *(channelIter->second);

      std::vector<uint8_t> descriptor = channel.send(
          tensor.data,
          tensor.length,
          channelSendCallbackWrapper_([sequenceNumber](Impl& impl) {
            impl.onSendOfTensorData_(sequenceNumber);
          }));
      messageBeingWritten.numTensorDataStillBeingSent++;
      pbTensorDescriptor->set_channel_name(name);
      // FIXME This makes a copy
      pbTensorDescriptor->set_channel_descriptor(
          descriptor.data(), descriptor.size());

      foundAChannel = true;
      break;
    }
    TP_THROW_ASSERT_IF(!foundAChannel);
  }

  connection_->write(
      *pbPacketOut,
      writePacketCallbackWrapper_([pbPacketOut](Impl& /* unused */) {}));

  connection_->write(
      message.data,
      message.length,
      writeCallbackWrapper_([sequenceNumber](Impl& impl) {
        impl.onWriteOfMessageData_(sequenceNumber);
      }));

  messageBeingWritten.message = std::move(message);
  messageBeingWritten.callback = std::move(fn);

  messagesBeingWritten_.push_back(std::move(messageBeingWritten));
}

void Pipe::Impl::onReadWhileServerWaitingForBrochure_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_BROCHURE);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kBrochure);
  const proto::Brochure& pbBrochure = pbPacketIn.brochure();

  auto pbPacketOut = std::make_shared<proto::Packet>();
  proto::BrochureAnswer* pbBrochureAnswer =
      pbPacketOut->mutable_brochure_answer();
  bool needToWaitForConnections = false;

  bool foundATransport = false;
  for (const auto& contextIter : context_->contextsByPriority_) {
    const std::string& transport = std::get<0>(contextIter.second);
    const transport::Context& context = *(std::get<1>(contextIter.second));

    // This pipe's listener might not have an address for that transport.
    const auto listenerIter = listener_->listeners_.find(transport);
    if (listenerIter == listener_->listeners_.cend()) {
      continue;
    }
    const transport::Listener& listener = *(listenerIter->second);

    const auto pbTransportAdvertisementIter =
        pbBrochure.transport_advertisement().find(transport);
    if (pbTransportAdvertisementIter ==
        pbBrochure.transport_advertisement().cend()) {
      continue;
    }
    const proto::TransportAdvertisement& pbTransportAdvertisement =
        pbTransportAdvertisementIter->second;
    const std::string& domainDescriptor =
        pbTransportAdvertisement.domain_descriptor();
    if (domainDescriptor != context.domainDescriptor()) {
      continue;
    }

    pbBrochureAnswer->set_transport(transport);
    pbBrochureAnswer->set_address(listener.addr());

    if (transport != transport_) {
      transport_ = transport;
      TP_DCHECK(!registrationId_.has_value());
      registrationId_.emplace(listener_->registerConnectionRequest_(
          connectionRequestCallbackWrapper_(
              [](Impl& impl,
                 std::string transport,
                 std::shared_ptr<transport::Connection> connection) {
                impl.onAcceptWhileServerWaitingForConnection_(
                    std::move(transport), std::move(connection));
              })));
      needToWaitForConnections = true;
      pbBrochureAnswer->set_registration_id(registrationId_.value());
    }

    foundATransport = true;
    break;
  }
  TP_THROW_ASSERT_IF(!foundATransport);

  auto pbAllChannelSelections = pbBrochureAnswer->mutable_channel_selection();
  for (const auto& channelFactoryIter : context_->channelFactoriesByPriority_) {
    const std::string& name = std::get<0>(channelFactoryIter.second);
    const channel::ChannelFactory& channelFactory =
        *(std::get<1>(channelFactoryIter.second));

    const auto pbChannelAdvertisementIter =
        pbBrochure.channel_advertisement().find(name);
    if (pbChannelAdvertisementIter ==
        pbBrochure.channel_advertisement().cend()) {
      continue;
    }
    const proto::ChannelAdvertisement& pbChannelAdvertisement =
        pbChannelAdvertisementIter->second;
    const std::string& domainDescriptor =
        pbChannelAdvertisement.domain_descriptor();
    if (domainDescriptor != channelFactory.domainDescriptor()) {
      continue;
    }

    channelRegistrationIds_[name] =
        listener_->registerConnectionRequest_(connectionRequestCallbackWrapper_(
            [name](
                Impl& impl,
                std::string transport,
                std::shared_ptr<transport::Connection> connection) {
              impl.onAcceptWhileServerWaitingForChannel_(
                  name, std::move(transport), std::move(connection));
            }));
    needToWaitForConnections = true;
    proto::ChannelSelection* pbChannelSelection =
        &(*pbAllChannelSelections)[name];
    pbChannelSelection->set_registration_id(channelRegistrationIds_[name]);
  }

  connection_->write(
      *pbPacketOut,
      writePacketCallbackWrapper_([pbPacketOut](Impl& /* unused */) {}));

  if (!needToWaitForConnections) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      connection_->read(
          *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
            impl.onReadOfMessageDescriptor_(*pbPacketIn);
          }));
      connectionState_ = NEXT_UP_IS_DATA;
    }
  } else {
    state_ = SERVER_WAITING_FOR_CONNECTIONS;
  }
}

void Pipe::Impl::onReadWhileClientWaitingForBrochureAnswer_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(state_, CLIENT_WAITING_FOR_BROCHURE_ANSWER);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kBrochureAnswer);

  const proto::BrochureAnswer& pbBrochureAnswer = pbPacketIn.brochure_answer();
  const std::string& transport = pbBrochureAnswer.transport();
  std::string address = pbBrochureAnswer.address();
  auto contextIter = context_->contexts_.find(transport);
  TP_DCHECK(contextIter != context_->contexts_.cend());
  transport::Context& context = *(contextIter->second);

  if (transport != transport_) {
    std::shared_ptr<transport::Connection> connection =
        context.connect(address);
    auto pbPacketOut = std::make_shared<proto::Packet>();
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut->mutable_requested_connection();
    pbRequestedConnection->set_registration_id(
        pbBrochureAnswer.registration_id());
    connection->write(
        *pbPacketOut,
        writePacketCallbackWrapper_([pbPacketOut](Impl& /* unused */) {}));

    transport_ = transport;
    connection_ = std::move(connection);
  }

  for (const auto& pbChannelSelectionIter :
       pbBrochureAnswer.channel_selection()) {
    const std::string& name = pbChannelSelectionIter.first;
    const proto::ChannelSelection& pbChannelSelection =
        pbChannelSelectionIter.second;

    auto channelFactoryIter = context_->channelFactories_.find(name);
    TP_DCHECK(channelFactoryIter != context_->channelFactories_.end());
    channel::ChannelFactory& channelFactory = *(channelFactoryIter->second);

    std::shared_ptr<transport::Connection> connection =
        context.connect(address);

    auto pbPacketOut = std::make_shared<proto::Packet>();
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut->mutable_requested_connection();
    pbRequestedConnection->set_registration_id(
        pbChannelSelection.registration_id());
    connection->write(
        *pbPacketOut,
        writePacketCallbackWrapper_([pbPacketOut](Impl& /* unused */) {}));

    channels_.emplace(
        name,
        channelFactory.createChannel(
            std::move(connection), channel::Channel::Endpoint::kConnect));
  }

  state_ = ESTABLISHED;
  doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
  if (!messagesBeingExpected_.empty()) {
    auto pbPacketIn2 = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn2, readPacketCallbackWrapper_([pbPacketIn2](Impl& impl) {
          impl.onReadOfMessageDescriptor_(*pbPacketIn2);
        }));
    connectionState_ = NEXT_UP_IS_DATA;
  }
}

void Pipe::Impl::onAcceptWhileServerWaitingForConnection_(
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  TP_DCHECK(registrationId_.has_value());
  listener_->unregisterConnectionRequest_(registrationId_.value());
  registrationId_.reset();
  TP_DCHECK_EQ(transport_, receivedTransport);
  connection_.reset();
  connection_ = std::move(receivedConnection);

  if (!registrationId_.has_value() && channelRegistrationIds_.empty()) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      connection_->read(
          *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
            impl.onReadOfMessageDescriptor_(*pbPacketIn);
          }));
      connectionState_ = NEXT_UP_IS_DATA;
    }
  }
}

void Pipe::Impl::onAcceptWhileServerWaitingForChannel_(
    std::string name,
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(inLoop_());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  auto channelRegistrationIdIter = channelRegistrationIds_.find(name);
  TP_DCHECK(channelRegistrationIdIter != channelRegistrationIds_.end());
  listener_->unregisterConnectionRequest_(channelRegistrationIdIter->second);
  channelRegistrationIds_.erase(channelRegistrationIdIter);

  TP_DCHECK_EQ(transport_, receivedTransport);
  auto channelIter = channels_.find(name);
  TP_DCHECK(channelIter == channels_.end());

  auto channelFactoryIter = context_->channelFactories_.find(name);
  TP_DCHECK(channelFactoryIter != context_->channelFactories_.end());
  std::shared_ptr<channel::ChannelFactory> channelFactory =
      channelFactoryIter->second;

  channels_.emplace(
      name,
      channelFactory->createChannel(
          std::move(receivedConnection), channel::Channel::Endpoint::kListen));

  if (!registrationId_.has_value() && channelRegistrationIds_.empty()) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      connection_->read(
          *pbPacketIn, readPacketCallbackWrapper_([pbPacketIn](Impl& impl) {
            impl.onReadOfMessageDescriptor_(*pbPacketIn);
          }));
      connectionState_ = NEXT_UP_IS_DATA;
    }
  }
}

void Pipe::Impl::onReadOfMessageDescriptor_(const proto::Packet& pbPacketIn) {
  TP_DCHECK(inLoop_());
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
  message.length = pbMessageDescriptor.size_in_bytes();
  messageBeingAllocated.length = message.length;
  message.metadata = pbMessageDescriptor.metadata();
  for (const auto& pbTensorDescriptor :
       pbMessageDescriptor.tensor_descriptors()) {
    Message::Tensor tensor;
    MessageBeingAllocated::Tensor tensorBeingAllocated;
    tensor.length = pbTensorDescriptor.size_in_bytes();
    tensorBeingAllocated.length = tensor.length;
    tensor.metadata = pbTensorDescriptor.metadata();
    tensorBeingAllocated.channelName = pbTensorDescriptor.channel_name();
    tensorBeingAllocated.channelDescriptor = std::vector<uint8_t>(
        pbTensorDescriptor.channel_descriptor().data(),
        pbTensorDescriptor.channel_descriptor().data() +
            pbTensorDescriptor.channel_descriptor().size());
    message.tensors.push_back(std::move(tensor));
    messageBeingAllocated.tensors.push_back(std::move(tensorBeingAllocated));
  }

  messagesBeingAllocated_.push_back(std::move(messageBeingAllocated));

  triggerReadDescriptorCallback_(
      messageBeingExpected.sequenceNumber,
      std::move(messageBeingExpected.callback),
      Error::kSuccess,
      std::move(message));
}

void Pipe::Impl::onReadOfMessageData_(int64_t sequenceNumber) {
  TP_DCHECK(inLoop_());
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.dataStillBeingRead = false;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onRecvOfTensorData_(int64_t sequenceNumber) {
  TP_DCHECK(inLoop_());
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.numTensorDataStillBeingReceived--;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onWriteOfMessageData_(int64_t sequenceNumber) {
  TP_DCHECK(inLoop_());
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.dataStillBeingWritten = false;
  triggerReadyCallbacks_();
}

void Pipe::Impl::onSendOfTensorData_(int64_t sequenceNumber) {
  TP_DCHECK(inLoop_());
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.numTensorDataStillBeingSent--;
  triggerReadyCallbacks_();
}

} // namespace tensorpipe
