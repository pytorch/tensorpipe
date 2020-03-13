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
#include <tensorpipe/core/core.pb.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/listener.h>

namespace tensorpipe {

//
// Initialization
//

std::shared_ptr<Pipe> Pipe::create(
    std::shared_ptr<Context> context,
    const std::string& url) {
  std::string transport;
  std::string address;
  std::tie(transport, address) = splitSchemeOfURL(url);
  std::shared_ptr<transport::Connection> connection =
      context->getContextForTransport_(transport)->connect(address);
  auto pipe = std::make_shared<Pipe>(
      ConstructorToken(), std::move(context), transport, std::move(connection));
  pipe->start_();
  return pipe;
}

Pipe::Pipe(
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
      connectionRequestCallbackWrapper_(*this),
      channelRecvCallbackWrapper_(*this),
      channelSendCallbackWrapper_(*this) {}

Pipe::Pipe(
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
      connectionRequestCallbackWrapper_(*this),
      channelRecvCallbackWrapper_(*this),
      channelSendCallbackWrapper_(*this) {}

void Pipe::start_() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (state_ == CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE) {
    auto pbPacketOut = std::make_shared<proto::Packet>();
    // This makes the packet contain a SpontaneousConnection message.
    pbPacketOut->mutable_spontaneous_connection();
    connection_->write(
        *pbPacketOut,
        writeCallbackWrapper_(
            [pbPacketOut](Pipe& /* unused */, TLock /* unused */) {}));

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
        writeCallbackWrapper_(
            [pbPacketOut2](Pipe& /* unused */, TLock /* unused */) {}));
    state_ = CLIENT_WAITING_FOR_BROCHURE_ANSWER;
    auto pbPacketIn = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn,
        readPacketCallbackWrapper_([pbPacketIn](Pipe& pipe, TLock lock) {
          pipe.onReadWhileClientWaitingForBrochureAnswer_(*pbPacketIn, lock);
        }));
  }
  if (state_ == SERVER_WAITING_FOR_BROCHURE) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn,
        readPacketCallbackWrapper_([pbPacketIn](Pipe& pipe, TLock lock) {
          pipe.onReadWhileServerWaitingForBrochure_(*pbPacketIn, lock);
        }));
  }
}

Pipe::~Pipe() {
  std::unique_lock<std::mutex> lock(mutex_);
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
  handleError_(lock);
}

//
// Entry points for user code
//

void Pipe::readDescriptor(read_descriptor_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  int64_t sequenceNumber = nextMessageBeingRead_++;

  if (error_) {
    triggerReadDescriptorCallback_(
        sequenceNumber, std::move(fn), error_, Message(), lock);
    return;
  }

  messagesBeingExpected_.push_back(
      MessageBeingExpected{sequenceNumber, std::move(fn)});

  if (messagesBeingExpected_.size() == 1 && state_ == ESTABLISHED &&
      connectionState_ == NEXT_UP_IS_DESCRIPTOR) {
    auto pbPacketIn = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn,
        readPacketCallbackWrapper_([pbPacketIn](Pipe& pipe, TLock lock) {
          pipe.onReadOfMessageDescriptor_(*pbPacketIn, lock);
        }));
    connectionState_ = NEXT_UP_IS_DATA;
  }
}

void Pipe::read(Message message, read_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  // This is such a bad logical error on the user's side that it doesn't deserve
  // to pass through the channel for "expected errors" (i.e., the callback).
  TP_THROW_ASSERT_IF(messagesBeingAllocated_.empty());

  MessageBeingAllocated messageBeingAllocated{
      std::move(messagesBeingAllocated_.front())};
  messagesBeingAllocated_.pop_front();

  int64_t sequenceNumber = messageBeingAllocated.sequenceNumber;

  if (error_) {
    triggerReadCallback_(
        sequenceNumber, std::move(fn), error_, std::move(message), lock);
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
      readCallbackWrapper_([sequenceNumber](
                               Pipe& pipe,
                               const void* /* unused */,
                               size_t /* unused */,
                               TLock lock) {
        pipe.onReadOfMessageData_(sequenceNumber, lock);
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
        channelRecvCallbackWrapper_([sequenceNumber](Pipe& pipe, TLock lock) {
          pipe.onRecvOfTensorData_(sequenceNumber, lock);
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
        *pbPacketIn,
        readPacketCallbackWrapper_([pbPacketIn](Pipe& pipe, TLock lock) {
          pipe.onReadOfMessageDescriptor_(*pbPacketIn, lock);
        }));
    connectionState_ = NEXT_UP_IS_DATA;
  }
}

void Pipe::write(Message message, write_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  int64_t sequenceNumber = nextMessageBeingWritten_++;

  if (error_) {
    triggerWriteCallback_(
        sequenceNumber, std::move(fn), error_, std::move(message), lock);
    return;
  }

  if (state_ == ESTABLISHED) {
    writeWhenEstablished_(
        sequenceNumber, std::move(message), std::move(fn), lock);
  } else {
    messagesBeingQueued_.push_back(
        MessageBeingQueued{sequenceNumber, std::move(message), std::move(fn)});
  }
}

//
// Helpers to schedule our callbacks into user code
//

void Pipe::triggerReadDescriptorCallback_(
    int64_t sequenceNumber,
    read_descriptor_callback_fn&& fn,
    const Error& error,
    Message message,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  readDescriptorCallbackCalled_.wait(lock, [&]() {
    return nextReadDescriptorCallbackToCall_ == sequenceNumber;
  });
  lock.unlock();
  fn(error, std::move(message));
  lock.lock();
  nextReadDescriptorCallbackToCall_++;
  readDescriptorCallbackCalled_.notify_all();
}

void Pipe::triggerReadCallback_(
    int64_t sequenceNumber,
    read_callback_fn&& fn,
    const Error& error,
    Message message,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  readCallbackCalled_.wait(
      lock, [&]() { return nextReadCallbackToCall_ == sequenceNumber; });
  lock.unlock();
  fn(error, std::move(message));
  lock.lock();
  nextReadCallbackToCall_++;
  readCallbackCalled_.notify_all();
}

void Pipe::triggerWriteCallback_(
    int64_t sequenceNumber,
    write_callback_fn&& fn,
    const Error& error,
    Message message,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  writeCallbackCalled_.wait(
      lock, [&]() { return nextWriteCallbackToCall_ == sequenceNumber; });
  lock.unlock();
  fn(error, std::move(message));
  lock.lock();
  nextWriteCallbackToCall_++;
  writeCallbackCalled_.notify_all();
}

//
// Error handling
//

void Pipe::handleError_(TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);

  while (!messagesBeingExpected_.empty()) {
    MessageBeingExpected m = std::move(messagesBeingExpected_.front());
    messagesBeingExpected_.pop_front();
    triggerReadDescriptorCallback_(
        m.sequenceNumber, std::move(m.callback), error_, Message(), lock);
  }
  while (!messagesBeingRead_.empty()) {
    MessageBeingRead m = std::move(messagesBeingRead_.front());
    messagesBeingRead_.pop_front();
    triggerReadCallback_(
        m.sequenceNumber,
        std::move(m.callback),
        error_,
        std::move(m.message),
        lock);
  }
  while (!messagesBeingWritten_.empty()) {
    MessageBeingWritten m = std::move(messagesBeingWritten_.front());
    messagesBeingWritten_.pop_front();
    triggerWriteCallback_(
        m.sequenceNumber,
        std::move(m.callback),
        error_,
        std::move(m.message),
        lock);
  }
  while (!messagesBeingQueued_.empty()) {
    MessageBeingQueued m = std::move(messagesBeingQueued_.front());
    messagesBeingQueued_.pop_front();
    triggerWriteCallback_(
        m.sequenceNumber,
        std::move(m.callback),
        error_,
        std::move(m.message),
        lock);
  }
}

//
// Everything else
//

void Pipe::doWritesAccumulatedWhileWaitingForPipeToBeEstablished_(TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  TP_DCHECK_EQ(state_, ESTABLISHED);
  while (!messagesBeingQueued_.empty()) {
    MessageBeingQueued m = std::move(messagesBeingQueued_.front());
    messagesBeingQueued_.pop_front();
    writeWhenEstablished_(
        m.sequenceNumber, std::move(m.message), std::move(m.callback), lock);
  }
}

void Pipe::writeWhenEstablished_(
    int64_t sequenceNumber,
    Message message,
    write_callback_fn fn,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
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
          channelSendCallbackWrapper_([sequenceNumber](Pipe& pipe, TLock lock) {
            pipe.onSendOfTensorData_(sequenceNumber, lock);
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
      writeCallbackWrapper_(
          [pbPacketOut](Pipe& /* unused */, TLock /* unused */) {}));

  connection_->write(
      message.data,
      message.length,
      writeCallbackWrapper_([sequenceNumber](Pipe& pipe, TLock lock) {
        pipe.onWriteOfMessageData_(sequenceNumber, lock);
      }));

  messageBeingWritten.message = std::move(message);
  messageBeingWritten.callback = std::move(fn);

  messagesBeingWritten_.push_back(std::move(messageBeingWritten));
}

void Pipe::onReadWhileServerWaitingForBrochure_(
    const proto::Packet& pbPacketIn,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
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
              [](Pipe& pipe,
                 std::string transport,
                 std::shared_ptr<transport::Connection> connection,
                 TLock lock) {
                pipe.onAcceptWhileServerWaitingForConnection_(
                    std::move(transport), std::move(connection), lock);
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
                Pipe& pipe,
                std::string transport,
                std::shared_ptr<transport::Connection> connection,
                TLock lock) {
              pipe.onAcceptWhileServerWaitingForChannel_(
                  name, std::move(transport), std::move(connection), lock);
            }));
    needToWaitForConnections = true;
    proto::ChannelSelection* pbChannelSelection =
        &(*pbAllChannelSelections)[name];
    pbChannelSelection->set_registration_id(channelRegistrationIds_[name]);
  }

  connection_->write(
      *pbPacketOut,
      writeCallbackWrapper_(
          [pbPacketOut](Pipe& /* unused */, TLock /* unused */) {}));

  if (!needToWaitForConnections) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_(lock);
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      connection_->read(
          *pbPacketIn,
          readPacketCallbackWrapper_([pbPacketIn](Pipe& pipe, TLock lock) {
            pipe.onReadOfMessageDescriptor_(*pbPacketIn, lock);
          }));
      connectionState_ = NEXT_UP_IS_DATA;
    }
  } else {
    state_ = SERVER_WAITING_FOR_CONNECTIONS;
  }
}

void Pipe::onReadWhileClientWaitingForBrochureAnswer_(
    const proto::Packet& pbPacketIn,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
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
        writeCallbackWrapper_(
            [pbPacketOut](Pipe& /* unused */, TLock /* unused */) {}));

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
        writeCallbackWrapper_(
            [pbPacketOut](Pipe& /* unused */, TLock /* unused */) {}));

    channels_.emplace(
        name,
        channelFactory.createChannel(
            std::move(connection), channel::Channel::Endpoint::kConnect));
  }

  state_ = ESTABLISHED;
  doWritesAccumulatedWhileWaitingForPipeToBeEstablished_(lock);
  TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
  if (!messagesBeingExpected_.empty()) {
    auto pbPacketIn2 = std::make_shared<proto::Packet>();
    connection_->read(
        *pbPacketIn2,
        readPacketCallbackWrapper_([pbPacketIn2](Pipe& pipe, TLock lock) {
          pipe.onReadOfMessageDescriptor_(*pbPacketIn2, lock);
        }));
    connectionState_ = NEXT_UP_IS_DATA;
  }
}

void Pipe::onAcceptWhileServerWaitingForConnection_(
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  TP_DCHECK(registrationId_.has_value());
  listener_->unregisterConnectionRequest_(registrationId_.value());
  registrationId_.reset();
  TP_DCHECK_EQ(transport_, receivedTransport);
  connection_.reset();
  connection_ = std::move(receivedConnection);

  if (!registrationId_.has_value() && channelRegistrationIds_.empty()) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_(lock);
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      connection_->read(
          *pbPacketIn,
          readPacketCallbackWrapper_([pbPacketIn](Pipe& pipe, TLock lock) {
            pipe.onReadOfMessageDescriptor_(*pbPacketIn, lock);
          }));
      connectionState_ = NEXT_UP_IS_DATA;
    }
  }
}

void Pipe::onAcceptWhileServerWaitingForChannel_(
    std::string name,
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
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
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_(lock);
    TP_DCHECK_EQ(connectionState_, NEXT_UP_IS_DESCRIPTOR);
    if (!messagesBeingExpected_.empty()) {
      auto pbPacketIn = std::make_shared<proto::Packet>();
      connection_->read(
          *pbPacketIn,
          readPacketCallbackWrapper_([pbPacketIn](Pipe& pipe, TLock lock) {
            pipe.onReadOfMessageDescriptor_(*pbPacketIn, lock);
          }));
      connectionState_ = NEXT_UP_IS_DATA;
    }
  }
}

void Pipe::onReadOfMessageDescriptor_(
    const proto::Packet& pbPacketIn,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
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
      std::move(message),
      lock);
}

void Pipe::onReadOfMessageData_(int64_t sequenceNumber, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.dataStillBeingRead = false;
  checkForMessagesDoneReading_(lock);
}

void Pipe::onRecvOfTensorData_(int64_t sequenceNumber, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.numTensorDataStillBeingReceived--;
  checkForMessagesDoneReading_(lock);
}

void Pipe::onWriteOfMessageData_(int64_t sequenceNumber, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.dataStillBeingWritten = false;
  checkForMessagesDoneWriting_(lock);
}

void Pipe::onSendOfTensorData_(int64_t sequenceNumber, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.numTensorDataStillBeingSent--;
  checkForMessagesDoneWriting_(lock);
}

void Pipe::checkForMessagesDoneReading_(TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  while (!messagesBeingRead_.empty()) {
    MessageBeingRead& messageBeingRead = messagesBeingRead_.front();
    if (messageBeingRead.dataStillBeingRead ||
        messageBeingRead.numTensorDataStillBeingReceived > 0) {
      break;
    }
    MessageBeingRead messageRead = std::move(messagesBeingRead_.front());
    messagesBeingRead_.pop_front();
    triggerReadCallback_(
        messageRead.sequenceNumber,
        std::move(messageRead.callback),
        Error::kSuccess,
        std::move(messageRead.message),
        lock);
  }
}

void Pipe::checkForMessagesDoneWriting_(TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  while (!messagesBeingWritten_.empty()) {
    MessageBeingWritten& messageBeingWritten = messagesBeingWritten_.front();
    if (messageBeingWritten.dataStillBeingWritten ||
        messageBeingWritten.numTensorDataStillBeingSent > 0) {
      break;
    }
    MessageBeingWritten messageWritten =
        std::move(messagesBeingWritten_.front());
    messagesBeingWritten_.pop_front();
    triggerWriteCallback_(
        messageWritten.sequenceNumber,
        std::move(messageWritten.callback),
        Error::kSuccess,
        std::move(messageWritten.message),
        lock);
  }
}

} // namespace tensorpipe
