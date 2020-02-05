/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/pipe.h>

#include <cstring>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/core/error_macros.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/proto/all.pb.h>

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
    auto allTransportAdvertisements =
        pbBrochure->mutable_transport_advertisement();
    for (const auto& contextIter : context_->contexts_) {
      const std::string& transport = contextIter.first;
      const transport::Context& context = *(contextIter.second);
      proto::TransportAdvertisement* transportAdvertisement =
          &(*allTransportAdvertisements)[transport];
      transportAdvertisement->set_domain_descriptor(context.domainDescriptor());
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
  // FIXME This should throw an exception or pass an error to the callback
  TP_DCHECK(!waitingDescriptors_.empty())
      << "called read with no waiting descriptor";
  Message expectedMessage{std::move(waitingDescriptors_.front())};
  waitingDescriptors_.pop_front();
  // TODO Compare message with expectedMessage
  proto::Packet pbPacketOut;
  // This makes the packet contain a Request message.
  pbPacketOut.mutable_request();
  connection_->write(pbPacketOut, wrapWriteCallback_());
  pendingReads_.emplace_back(std::move(message), std::move(fn));
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
    bound_read_packet_callback_fn fn,
    const transport::Error& error,
    const proto::Packet& packet) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = TP_CREATE_ERROR(TransportError, error);
    flushEverythingOnError_();
    return;
  }
  if (fn) {
    fn(*this, packet);
  }
}

void Pipe::writeCallbackEntryPoint_(
    bound_write_callback_fn fn,
    const transport::Error& error) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = TP_CREATE_ERROR(TransportError, error);
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

//
// Helpers to prepare callbacks from transports
//

Pipe::transport_write_callback_fn Pipe::wrapWriteCallback_(
    bound_write_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(Pipe&, const transport::Error&)>(
          [fn{std::move(fn)}](
              Pipe& pipe, const transport::Error& error) mutable {
            pipe.writeCallbackEntryPoint_(std::move(fn), error);
          }));
}

Pipe::transport_read_packet_callback_fn Pipe::wrapReadPacketCallback_(
    bound_read_packet_callback_fn fn) {
  return runIfAlive(
      *this,
      std::function<void(Pipe&, const transport::Error&, const proto::Packet&)>(
          [fn{std::move(fn)}](
              Pipe& pipe,
              const transport::Error& error,
              const proto::Packet& packet) mutable {
            pipe.readCallbackEntryPoint_(std::move(fn), error, packet);
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

//
// Helpers to schedule our callbacks into user code
//

void Pipe::triggerReadDescriptorCallback_(
    read_descriptor_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  scheduledReadDescriptorCallbacks_.schedule(
      std::move(fn), error, std::move(message));
  triggerRunOfScheduledCallbacks_();
}

void Pipe::triggerReadCallback_(
    read_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  scheduledReadCallbacks_.schedule(std::move(fn), error, std::move(message));
  triggerRunOfScheduledCallbacks_();
}

void Pipe::triggerWriteCallback_(
    write_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  scheduledWriteCallbacks_.schedule(std::move(fn), error, std::move(message));
  triggerRunOfScheduledCallbacks_();
}

void Pipe::triggerRunOfScheduledCallbacks_() {
  if (!isRunOfScheduledCallbacksTriggered_.test_and_set()) {
    context_->callCallback_(
        runIfAlive(*this, std::function<void(Pipe&)>([](Pipe& pipe) {
          pipe.isRunOfScheduledCallbacksTriggered_.clear();
          pipe.runScheduledCallbacks_();
        })));
  }
}

void Pipe::runScheduledCallbacks_() {
  scheduledReadDescriptorCallbacks_.run();
  scheduledReadCallbacks_.run();
  scheduledWriteCallbacks_.run();
}

//
// Error handling
//

void Pipe::flushEverythingOnError_() {
  readDescriptorCallback_.triggerIfArmed(error_, Message());
  waitingDescriptors_.clear();
  while (!pendingReads_.empty()) {
    Message message{std::move(std::get<0>(pendingReads_.front()))};
    read_callback_fn fn{std::move(std::get<1>(pendingReads_.front()))};
    pendingReads_.pop_front();
    scheduledReadCallbacks_.schedule(std::move(fn), error_, std::move(message));
  }
  while (!pendingWrites_.empty()) {
    Message message{std::move(std::get<0>(pendingWrites_.front()))};
    write_callback_fn fn{std::move(std::get<1>(pendingWrites_.front()))};
    pendingWrites_.pop_front();
    scheduledWriteCallbacks_.schedule(
        std::move(fn), error_, std::move(message));
  }
  while (!completingWrites_.empty()) {
    Message message{std::move(std::get<0>(completingWrites_.front()))};
    write_callback_fn fn{std::move(std::get<1>(completingWrites_.front()))};
    completingWrites_.pop_front();
    scheduledWriteCallbacks_.schedule(
        std::move(fn), error_, std::move(message));
  }
  triggerRunOfScheduledCallbacks_();
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
  proto::Packet pbPacketOut;
  proto::MessageDescriptor* pbMessageDesc =
      pbPacketOut.mutable_message_descriptor();
  pbMessageDesc->set_size_in_bytes(message.length);
  for (const auto& tensor : message.tensors) {
    proto::MessageDescriptor::TensorDescriptor* pbTensorDesc =
        pbMessageDesc->add_tensor_descriptors();
    pbTensorDesc->set_device_type(proto::DeviceType::DEVICE_TYPE_CPU);
    pbTensorDesc->set_size_in_bytes(tensor.length);
    pbTensorDesc->set_user_data(tensor.metadata);
  }
  pendingWrites_.emplace_back(std::move(message), std::move(fn));
  connection_->write(pbPacketOut, wrapWriteCallback_());
}

void Pipe::onReadWhileServerWaitingForBrochure_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_BROCHURE);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kBrochure);
  const proto::Brochure& pbBrochure = pbPacketIn.brochure();

  // FIXME This is hardcoded logic, for now...
  std::string chosenTransport = "shm";
  const auto chosenTransportAdvertisementIter =
      pbBrochure.transport_advertisement().find(chosenTransport);
  TP_DCHECK(
      chosenTransportAdvertisementIter !=
      pbBrochure.transport_advertisement().cend());

  if (chosenTransport == transport_) {
    proto::Packet pbPacketOut;
    proto::BrochureAnswer* pbBrochureAnswer =
        pbPacketOut.mutable_brochure_answer();
    pbBrochureAnswer->set_transport(transport_);
    connection_->write(pbPacketOut, wrapWriteCallback_());
    state_ = ESTABLISHED;
    doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
    connection_->read(wrapReadPacketCallback_(
        [](Pipe& pipe, const proto::Packet& pbPacketIn) {
          pipe.onReadWhenEstablished_(pbPacketIn);
        }));
  } else {
    initialConnection_ = std::move(connection_);
    connection_.reset();
    transport_ = chosenTransport;

    const proto::TransportAdvertisement& chosenTransportAdvertisement =
        chosenTransportAdvertisementIter->second;
    std::string chosenDomainDescriptor =
        chosenTransportAdvertisement.domain_descriptor();
    auto chosenContextIter = context_->contexts_.find(chosenTransport);
    TP_DCHECK(chosenContextIter != context_->contexts_.end());
    auto chosenContext = chosenContextIter->second;
    TP_DCHECK_EQ(chosenContext->domainDescriptor(), chosenDomainDescriptor);

    const auto listenerIter = listener_->listeners_.find(chosenTransport);
    TP_DCHECK(listenerIter != listener_->listeners_.cend());
    const transport::Listener& listener = *(listenerIter->second);

    registrationId_.emplace(
        listener_->registerConnectionRequest_(wrapAcceptCallback_(
            [](Pipe& pipe,
               std::string transport,
               std::shared_ptr<transport::Connection> connection) {
              pipe.onAcceptWhileServerWaitingForConnection_(
                  std::move(transport), std::move(connection));
            })));

    proto::Packet pbPacketOut;
    proto::BrochureAnswer* pbBrochureAnswer =
        pbPacketOut.mutable_brochure_answer();
    pbBrochureAnswer->set_transport(chosenTransport);
    pbBrochureAnswer->set_address(listener.addr());
    pbBrochureAnswer->set_registration_id(registrationId_.value());
    initialConnection_->write(pbPacketOut, wrapWriteCallback_());
    state_ = SERVER_WAITING_FOR_CONNECTION;
  }
}

void Pipe::onReadWhileClientWaitingForBrochureAnswer_(
    const proto::Packet& pbPacketIn) {
  TP_DCHECK_EQ(state_, CLIENT_WAITING_FOR_BROCHURE_ANSWER);
  TP_DCHECK_EQ(pbPacketIn.type_case(), proto::Packet::kBrochureAnswer);

  const proto::BrochureAnswer& pbBrochureAnswer = pbPacketIn.brochure_answer();
  const std::string& chosenTransport = pbBrochureAnswer.transport();

  if (chosenTransport != transport_) {
    std::string chosenAddress = pbBrochureAnswer.address();
    uint64_t registrationId = pbBrochureAnswer.registration_id();

    auto chosenContextIter = context_->contexts_.find(chosenTransport);
    TP_DCHECK(chosenContextIter != context_->contexts_.end());
    auto chosenContext = chosenContextIter->second;
    auto chosenConnection = chosenContext->connect(chosenAddress);

    connection_.reset();
    connection_ = std::move(chosenConnection);

    proto::Packet pbPacketOut;
    proto::RequestedConnection* pbRequestedConnection =
        pbPacketOut.mutable_requested_connection();
    pbRequestedConnection->set_registration_id(registrationId);
    connection_->write(pbPacketOut, wrapWriteCallback_());
  }

  state_ = ESTABLISHED;
  doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  connection_->read(
      wrapReadPacketCallback_([](Pipe& pipe, const proto::Packet& pbPacketIn) {
        pipe.onReadWhenEstablished_(pbPacketIn);
      }));
}

void Pipe::onAcceptWhileServerWaitingForConnection_(
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTION);
  TP_DCHECK(registrationId_.has_value());
  listener_->unregisterConnectionRequest_(registrationId_.value());
  registrationId_.reset();
  TP_DCHECK_EQ(transport_, receivedTransport);
  TP_DCHECK(connection_ == nullptr);
  connection_ = std::move(receivedConnection);

  state_ = ESTABLISHED;
  doWritesAccumulatedWhileWaitingForPipeToBeEstablished_();
  connection_->read(
      wrapReadPacketCallback_([](Pipe& pipe, const proto::Packet& pbPacketIn) {
        pipe.onReadWhenEstablished_(pbPacketIn);
      }));
}

void Pipe::onReadWhenEstablished_(const proto::Packet& pbPacketIn) {
  TP_DCHECK_EQ(state_, ESTABLISHED);
  if (pbPacketIn.has_message_descriptor()) {
    const proto::MessageDescriptor& pbMessageDesc =
        pbPacketIn.message_descriptor();
    Message message;
    message.length = pbMessageDesc.size_in_bytes();
    for (const auto& pbTensorDesc : pbMessageDesc.tensor_descriptors()) {
      Message::Tensor tensor;
      tensor.length = pbTensorDesc.size_in_bytes();
      tensor.metadata = std::move(pbTensorDesc.user_data());
      message.tensors.push_back(std::move(tensor));
    }
    waitingDescriptors_.push_back(message.copyWithoutData());
    readDescriptorCallback_.trigger(Error::kSuccess, std::move(message));
  } else if (pbPacketIn.has_message()) {
    const proto::Message& pbMessage = pbPacketIn.message();
    TP_DCHECK(!pendingReads_.empty()) << "got message when no pending reads";
    Message message{std::move(std::get<0>(pendingReads_.front()))};
    read_callback_fn fn{std::move(std::get<1>(pendingReads_.front()))};
    pendingReads_.pop_front();
    std::memcpy(message.data.get(), pbMessage.data().data(), message.length);
    TP_DCHECK_EQ(pbMessage.tensors_size(), message.tensors.size())
        << "mismatch in number of tensors";
    for (int i = 0; i < message.tensors.size(); i += 1) {
      const proto::Message::Tensor& pbTensor = pbMessage.tensors(i);
      Message::Tensor& tensor = message.tensors[i];
      std::memcpy(tensor.data.get(), pbTensor.data().data(), tensor.length);
    }
    triggerReadCallback_(std::move(fn), Error::kSuccess, std::move(message));
  } else if (pbPacketIn.has_request()) {
    TP_DCHECK(!pendingWrites_.empty()) << "got request when no pending writes";
    Message message{std::move(std::get<0>(pendingWrites_.front()))};
    write_callback_fn fn{std::move(std::get<1>(pendingWrites_.front()))};
    pendingWrites_.pop_front();
    proto::Packet pbPacketOut;
    proto::Message* pbMessage = pbPacketOut.mutable_message();
    pbMessage->set_data(message.data.get(), message.length);
    for (const auto& tensor : message.tensors) {
      proto::Message::Tensor* pbTensor = pbMessage->add_tensors();
      pbTensor->set_data(tensor.data.get(), tensor.length);
    }
    connection_->write(
        pbPacketOut, wrapWriteCallback_([](Pipe& pipe) {
          TP_DCHECK(!pipe.completingWrites_.empty())
              << "got message when no completing writes";
          Message message =
              std::move(std::get<0>(pipe.completingWrites_.front()));
          write_callback_fn fn =
              std::move(std::get<1>(pipe.completingWrites_.front()));
          pipe.completingWrites_.pop_front();
          pipe.triggerWriteCallback_(
              std::move(fn), Error::kSuccess, std::move(message));
        }));
    completingWrites_.emplace_back(std::move(message), std::move(fn));
  } else {
    TP_THROW_ASSERT() << "unexpected type " << pbPacketIn.type_case();
  }
  connection_->read(
      wrapReadPacketCallback_([](Pipe& pipe, const proto::Packet& pbPacketIn) {
        pipe.onReadWhenEstablished_(pbPacketIn);
      }));
}

} // namespace tensorpipe
