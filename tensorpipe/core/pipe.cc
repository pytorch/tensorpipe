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
      connection_(std::move(connection)) {}

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
      connection_(std::move(connection)) {}

void Pipe::start_() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (state_ == CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE) {
    proto::Packet pbPacketOut;
    // This makes the packet contain a SpontaneousConnection message.
    pbPacketOut.mutable_spontaneous_connection();
    connection_->write(pbPacketOut, wrapWriteCallback_());
    pbPacketOut.Clear();
    proto::Brochure* pbBrochure = pbPacketOut.mutable_brochure();
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
    connection_->write(pbPacketOut, wrapWriteCallback_());
    state_ = CLIENT_WAITING_FOR_BROCHURE_ANSWER;
    connection_->read(wrapReadPacketCallback_(
        [](Pipe& pipe, const proto::Packet& pbPacketIn) {
          pipe.onReadWhileClientWaitingForBrochureAnswer_(pbPacketIn);
        }));
  }
  if (state_ == SERVER_WAITING_FOR_BROCHURE) {
    connection_->read(wrapReadPacketCallback_(
        [](Pipe& pipe, const proto::Packet& pbPacketIn) {
          pipe.onReadWhileServerWaitingForBrochure_(pbPacketIn);
        }));
  }
}

Pipe::~Pipe() {
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
  flushEverythingOnError_();
}

//
// Entry points for user code
//

void Pipe::readDescriptor(read_descriptor_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (error_) {
    triggerReadDescriptorCallback_(std::move(fn), error_, Message());
    return;
  }

  readDescriptorCallback_.arm(runIfAlive(
      *this,
      std::function<void(Pipe&, const Error&, Message&&)>(
          [fn{std::move(fn)}](
              Pipe& pipe, const Error& error, Message&& message) mutable {
            pipe.triggerReadDescriptorCallback_(
                std::move(fn), error, std::move(message));
          })));
}

void Pipe::read(Message&& message, read_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (error_) {
    triggerReadCallback_(std::move(fn), error_, std::move(message));
    return;
  }

  // This is such a bad logical error on the user's side that it doesn't deserve
  // to pass through the channel for "expected errors" (i.e., the callback).
  TP_THROW_ASSERT_IF(messagesBeingAllocated_.empty());

  MessageBeingAllocated messageBeingAllocated{
      std::move(messagesBeingAllocated_.front())};
  messagesBeingAllocated_.pop_front();

  MessageBeingRead messageBeingRead;

  messageBeingRead.sequenceNumber = nextMessageBeingRead_++;
  TP_DCHECK_GE(messageBeingAllocated.length, 0);
  TP_THROW_ASSERT_IF(message.length != messageBeingAllocated.length);
  connection_->read(
      message.data.get(),
      message.length,
      wrapReadCallback_(
          [sequenceNumber{messageBeingRead.sequenceNumber}](
              Pipe& pipe, const void* /* unused */, size_t /* unused */) {
            pipe.onReadOfMessageData_(sequenceNumber);
          }));

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
        tensor.data.get(),
        tensor.length,
        wrapChannelRecvCallback_(
            [sequenceNumber{messageBeingRead.sequenceNumber}](Pipe& pipe) {
              pipe.onRecvOfTensorData_(sequenceNumber);
            }));
    messageBeingRead.numTensorDataStillBeingReceived++;
  }

  messageBeingRead.message = std::move(message);
  messageBeingRead.callback = std::move(fn);

  messagesBeingRead_.push_back(std::move(messageBeingRead));

  connection_->read(
      wrapReadPacketCallback_([](Pipe& pipe, const proto::Packet& pbPacketIn) {
        pipe.onReadOfMessageDescriptor_(pbPacketIn);
      }));
}

void Pipe::write(Message&& message, write_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (error_) {
    triggerWriteCallback_(std::move(fn), error_, std::move(message));
    return;
  }

  if (state_ == ESTABLISHED) {
    writeWhenEstablished_(std::move(message), std::move(fn));
  } else {
    writesWaitingUntilPipeIsEstablished_.emplace_back(
        std::move(message), std::move(fn));
  }
}

//
// Entry points for callbacks from transports and listener
//

void Pipe::readCallbackEntryPoint_(
    bound_read_callback_fn fn,
    const Error& error,
    const void* ptr,
    size_t len) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = error;
    flushEverythingOnError_();
    return;
  }
  if (fn) {
    fn(*this, ptr, len);
  }
}

void Pipe::readPacketCallbackEntryPoint_(
    bound_read_packet_callback_fn fn,
    const Error& error,
    const proto::Packet& packet) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = error;
    flushEverythingOnError_();
    return;
  }
  if (fn) {
    fn(*this, packet);
  }
}

void Pipe::writeCallbackEntryPoint_(
    bound_write_callback_fn fn,
    const Error& error) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = error;
    flushEverythingOnError_();
    return;
  }
  if (fn) {
    fn(*this);
  }
}

void Pipe::acceptCallbackEntryPoint_(
    bound_accept_callback_fn fn,
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (fn) {
    fn(*this, std::move(transport), std::move(connection));
  }
}

void Pipe::channelRecvCallbackEntryPoint_(
    bound_channel_recv_callback_fn fn,
    const Error& error) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = error;
    flushEverythingOnError_();
    return;
  }
  if (fn) {
    fn(*this);
  }
}

void Pipe::channelSendCallbackEntryPoint_(
    bound_channel_send_callback_fn fn,
    const Error& error) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = error;
    flushEverythingOnError_();
    return;
  }
  if (fn) {
    fn(*this);
  }
}

//
// Helpers to prepare callbacks from transports
//

Pipe::transport_read_callback_fn Pipe::wrapReadCallback_(
    bound_read_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(Pipe&, const Error&, const void*, size_t)>(
          [fn{std::move(fn)}](
              Pipe& pipe,
              const Error& error,
              const void* ptr,
              size_t len) mutable {
            pipe.readCallbackEntryPoint_(std::move(fn), error, ptr, len);
          }));
}

Pipe::transport_read_packet_callback_fn Pipe::wrapReadPacketCallback_(
    bound_read_packet_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(Pipe&, const Error&, const proto::Packet&)>(
          [fn{std::move(fn)}](
              Pipe& pipe,
              const Error& error,
              const proto::Packet& packet) mutable {
            pipe.readPacketCallbackEntryPoint_(std::move(fn), error, packet);
          }));
}

Pipe::transport_write_callback_fn Pipe::wrapWriteCallback_(
    bound_write_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(Pipe&, const Error&)>(
          [fn{std::move(fn)}](Pipe& pipe, const Error& error) mutable {
            pipe.writeCallbackEntryPoint_(std::move(fn), error);
          }));
}

Pipe::accept_callback_fn Pipe::wrapAcceptCallback_(
    bound_accept_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(
          Pipe&, std::string, std::shared_ptr<transport::Connection>)>(
          [fn{std::move(fn)}](
              Pipe& pipe,
              std::string transport,
              std::shared_ptr<transport::Connection> connection) mutable {
            pipe.acceptCallbackEntryPoint_(
                std::move(fn), std::move(transport), std::move(connection));
          }));
}

Pipe::channel_recv_callback_fn Pipe::wrapChannelRecvCallback_(
    bound_channel_recv_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(Pipe&, const Error&)>(
          [fn{std::move(fn)}](Pipe& pipe, const Error& error) mutable {
            pipe.channelRecvCallbackEntryPoint_(std::move(fn), error);
          }));
}

Pipe::channel_send_callback_fn Pipe::wrapChannelSendCallback_(
    bound_channel_send_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(Pipe&, const Error&)>(
          [fn{std::move(fn)}](Pipe& pipe, const Error& error) mutable {
            pipe.channelSendCallbackEntryPoint_(std::move(fn), error);
          }));
}

//
// Helpers to schedule our callbacks into user code
//

void Pipe::triggerReadDescriptorCallback_(
    read_descriptor_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  // Capturing a Message makes the closure non-copyable so we need this wrapper.
  std::shared_ptr<Message> sharedMessage =
      std::make_shared<Message>(std::move(message));
  context_->callCallback_([fn{std::move(fn)},
                           error,
                           sharedMessage{std::move(sharedMessage)}]() mutable {
    fn(error, std::move(*sharedMessage));
  });
}

void Pipe::triggerReadCallback_(
    read_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  // Capturing a Message makes the closure non-copyable so we need this wrapper.
  std::shared_ptr<Message> sharedMessage =
      std::make_shared<Message>(std::move(message));
  context_->callCallback_([fn{std::move(fn)},
                           error,
                           sharedMessage{std::move(sharedMessage)}]() mutable {
    fn(error, std::move(*sharedMessage));
  });
}

void Pipe::triggerWriteCallback_(
    write_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  // Capturing a Message makes the closure non-copyable so we need this wrapper.
  std::shared_ptr<Message> sharedMessage =
      std::make_shared<Message>(std::move(message));
  context_->callCallback_([fn{std::move(fn)},
                           error,
                           sharedMessage{std::move(sharedMessage)}]() mutable {
    fn(error, std::move(*sharedMessage));
  });
}

//
// Error handling
//

void Pipe::flushEverythingOnError_() {
  readDescriptorCallback_.triggerAll(
      [&]() { return std::make_tuple(error_, Message()); });
  messagesBeingAllocated_.clear();
  while (!messagesBeingRead_.empty()) {
    Message message{std::move(messagesBeingRead_.front().message)};
    read_callback_fn fn{std::move(messagesBeingRead_.front().callback)};
    messagesBeingRead_.pop_front();
    triggerReadCallback_(std::move(fn), error_, std::move(message));
  }
  while (!writesWaitingUntilPipeIsEstablished_.empty()) {
    Message message{
        std::move(std::get<0>(writesWaitingUntilPipeIsEstablished_.front()))};
    write_callback_fn fn{
        std::move(std::get<1>(writesWaitingUntilPipeIsEstablished_.front()))};
    writesWaitingUntilPipeIsEstablished_.pop_front();
    triggerWriteCallback_(std::move(fn), error_, std::move(message));
  }
  while (!messagesBeingWritten_.empty()) {
    Message message{std::move(messagesBeingWritten_.front().message)};
    write_callback_fn fn{std::move(messagesBeingWritten_.front().callback)};
    messagesBeingWritten_.pop_front();
    triggerWriteCallback_(std::move(fn), error_, std::move(message));
  }
}

//
// Everything else
//

void Pipe::doWritesAccumulatedWhileWaitingForPipeToBeEstablished_() {
  TP_DCHECK_EQ(state_, ESTABLISHED);
  Message message;
  write_callback_fn fn;
  while (!writesWaitingUntilPipeIsEstablished_.empty()) {
    std::tie(message, fn) =
        std::move(writesWaitingUntilPipeIsEstablished_.front());
    writesWaitingUntilPipeIsEstablished_.pop_front();
    writeWhenEstablished_(std::move(message), std::move(fn));
  }
}

void Pipe::writeWhenEstablished_(Message&& message, write_callback_fn fn) {
  TP_DCHECK_EQ(state_, ESTABLISHED);

  MessageBeingWritten messageBeingWritten;
  proto::Packet pbPacketOut;
  proto::MessageDescriptor* pbMessageDescriptor =
      pbPacketOut.mutable_message_descriptor();

  messageBeingWritten.sequenceNumber = nextMessageBeingWritten_++;
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
          tensor.data.get(),
          tensor.length,
          wrapChannelSendCallback_(
              [sequenceNumber{messageBeingWritten.sequenceNumber}](Pipe& pipe) {
                pipe.onSendOfTensorData_(sequenceNumber);
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

  connection_->write(pbPacketOut, wrapWriteCallback_());
  connection_->write(
      message.data.get(),
      message.length,
      wrapWriteCallback_(
          [sequenceNumber{messageBeingWritten.sequenceNumber}](Pipe& pipe) {
            pipe.onWriteOfMessageData_(sequenceNumber);
          }));

  messageBeingWritten.message = std::move(message);
  messageBeingWritten.callback = std::move(fn);

  messagesBeingWritten_.push_back(std::move(messageBeingWritten));
}

void Pipe::onReadWhileServerWaitingForBrochure_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_BROCHURE);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kBrochure);
  const proto::Brochure& pbBrochure = pbPacketIn.brochure();

  proto::Packet pbPacketOut;
  proto::BrochureAnswer* pbBrochureAnswer =
      pbPacketOut.mutable_brochure_answer();
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
      registrationId_.emplace(
          listener_->registerConnectionRequest_(wrapAcceptCallback_(
              [](Pipe& pipe,
                 std::string transport,
                 std::shared_ptr<transport::Connection> connection) {
                pipe.onAcceptWhileServerWaitingForConnection_(
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
        listener_->registerConnectionRequest_(wrapAcceptCallback_(
            [name](
                Pipe& pipe,
                std::string transport,
                std::shared_ptr<transport::Connection> connection) {
              pipe.onAcceptWhileServerWaitingForChannel_(
                  name, std::move(transport), std::move(connection));
            }));
    needToWaitForConnections = true;
    proto::ChannelSelection* pbChannelSelection =
        &(*pbAllChannelSelections)[name];
    pbChannelSelection->set_registration_id(channelRegistrationIds_[name]);
  }

  connection_->write(pbPacketOut, wrapWriteCallback_());

  if (!needToWaitForConnections) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    connection_->read(wrapReadPacketCallback_(
        [](Pipe& pipe, const proto::Packet& pbPacketIn) {
          pipe.onReadOfMessageDescriptor_(pbPacketIn);
        }));
  } else {
    state_ = SERVER_WAITING_FOR_CONNECTIONS;
  }
}

void Pipe::onReadWhileClientWaitingForBrochureAnswer_(
    const proto::Packet& pbPacketIn) {
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

    proto::Packet pbPacketOut;
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut.mutable_requested_connection();
    pbRequestedConnection->set_registration_id(
        pbBrochureAnswer.registration_id());
    connection->write(pbPacketOut, wrapWriteCallback_());

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

    proto::Packet pbPacketOut;
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut.mutable_requested_connection();
    pbRequestedConnection->set_registration_id(
        pbChannelSelection.registration_id());
    connection->write(pbPacketOut, wrapWriteCallback_());

    channels_.emplace(
        name, channelFactory.createChannel(std::move(connection)));
  }

  state_ = ESTABLISHED;
  doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  connection_->read(
      wrapReadPacketCallback_([](Pipe& pipe, const proto::Packet& pbPacketIn) {
        pipe.onReadOfMessageDescriptor_(pbPacketIn);
      }));
}

void Pipe::onAcceptWhileServerWaitingForConnection_(
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
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
    connection_->read(wrapReadPacketCallback_(
        [](Pipe& pipe, const proto::Packet& pbPacketIn) {
          pipe.onReadOfMessageDescriptor_(pbPacketIn);
        }));
  }
}

void Pipe::onAcceptWhileServerWaitingForChannel_(
    std::string name,
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
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
      name, channelFactory->createChannel(std::move(receivedConnection)));

  if (!registrationId_.has_value() && channelRegistrationIds_.empty()) {
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    connection_->read(wrapReadPacketCallback_(
        [](Pipe& pipe, const proto::Packet& pbPacketIn) {
          pipe.onReadOfMessageDescriptor_(pbPacketIn);
        }));
  }
}

void Pipe::onReadOfMessageDescriptor_(const proto::Packet& pbPacketIn) {
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kMessageDescriptor);

  const proto::MessageDescriptor& pbMessageDescriptor =
      pbPacketIn.message_descriptor();
  Message message;
  MessageBeingAllocated messageBeingAllocated;
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
  readDescriptorCallback_.trigger(Error::kSuccess, std::move(message));
}

void Pipe::onReadOfMessageData_(int64_t sequenceNumber) {
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.dataStillBeingRead = false;
  checkForMessagesDoneReading_();
}

void Pipe::onRecvOfTensorData_(int64_t sequenceNumber) {
  auto iter = std::find_if(
      messagesBeingRead_.begin(), messagesBeingRead_.end(), [&](const auto& m) {
        return m.sequenceNumber == sequenceNumber;
      });
  TP_DCHECK(iter != messagesBeingRead_.end());
  MessageBeingRead& messageBeingRead = *iter;
  messageBeingRead.numTensorDataStillBeingReceived--;
  checkForMessagesDoneReading_();
}

void Pipe::onWriteOfMessageData_(int64_t sequenceNumber) {
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.dataStillBeingWritten = false;
  checkForMessagesDoneWriting_();
}

void Pipe::onSendOfTensorData_(int64_t sequenceNumber) {
  auto iter = std::find_if(
      messagesBeingWritten_.begin(),
      messagesBeingWritten_.end(),
      [&](const auto& m) { return m.sequenceNumber == sequenceNumber; });
  TP_DCHECK(iter != messagesBeingWritten_.end());
  MessageBeingWritten& messageBeingWritten = *iter;
  messageBeingWritten.numTensorDataStillBeingSent--;
  checkForMessagesDoneWriting_();
}

void Pipe::checkForMessagesDoneReading_() {
  while (!messagesBeingRead_.empty()) {
    MessageBeingRead& messageBeingRead = messagesBeingRead_.front();
    if (messageBeingRead.dataStillBeingRead ||
        messageBeingRead.numTensorDataStillBeingReceived > 0) {
      break;
    }
    MessageBeingRead messageRead = std::move(messagesBeingRead_.front());
    messagesBeingRead_.pop_front();
    triggerReadCallback_(
        std::move(messageRead.callback),
        Error::kSuccess,
        std::move(messageRead.message));
  }
}

void Pipe::checkForMessagesDoneWriting_() {
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
        std::move(messageWritten.callback),
        Error::kSuccess,
        std::move(messageWritten.message));
  }
}

} // namespace tensorpipe
