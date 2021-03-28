/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/pipe_impl.h>

#include <map>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <utility>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/core/context_impl.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/listener_impl.h>
#include <tensorpipe/transport/connection.h>

#include <tensorpipe/common/buffer.h>
#include <tensorpipe/common/cpu_buffer.h>
#if TENSORPIPE_SUPPORTS_CUDA
#include <tensorpipe/common/cuda_buffer.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

namespace tensorpipe {

namespace {

// Copy the payload and tensors sizes, the tensor descriptors, etc. from the
// message descriptor that is contained in the nop object to the ReadOperation.
void parseDescriptorOfMessage(ReadOperation& op, const Packet& nopPacketIn) {
  Message& message = op.message;

  TP_DCHECK_EQ(nopPacketIn.index(), nopPacketIn.index_of<MessageDescriptor>());
  const MessageDescriptor& nopMessageDescriptor =
      *nopPacketIn.get<MessageDescriptor>();

  message.metadata = nopMessageDescriptor.metadata;
  for (const auto& nopPayloadDescriptor :
       nopMessageDescriptor.payloadDescriptors) {
    Message::Payload payload;
    ReadOperation::Payload payloadBeingAllocated;
    payload.length = nopPayloadDescriptor.sizeInBytes;
    payloadBeingAllocated.length = payload.length;
    payload.metadata = nopPayloadDescriptor.metadata;
    message.payloads.push_back(std::move(payload));
    op.payloads.push_back(std::move(payloadBeingAllocated));
  }

  for (const auto& nopTensorDescriptor :
       nopMessageDescriptor.tensorDescriptors) {
    ReadOperation::Tensor tensorBeingAllocated;
    tensorBeingAllocated.length = nopTensorDescriptor.sizeInBytes;
    tensorBeingAllocated.channelName = nopTensorDescriptor.channelName;

    message.tensors.emplace_back();
    Message::Tensor& tensor = message.tensors.back();
    op.tensors.push_back(std::move(tensorBeingAllocated));
    tensor.metadata = nopTensorDescriptor.metadata;
    switch (nopTensorDescriptor.deviceType) {
      case DeviceType::kCpu: {
        CpuBuffer buffer;
        buffer.length = static_cast<size_t>(tensorBeingAllocated.length);
        tensor.buffer = buffer;
        break;
      }
#if TENSORPIPE_SUPPORTS_CUDA
      case DeviceType::kCuda: {
        CudaBuffer buffer;
        buffer.length = static_cast<size_t>(tensorBeingAllocated.length);
        tensor.buffer = buffer;
        break;
      }
#endif // TENSORPIPE_SUPPORTS_CUDA
      default:
        TP_THROW_ASSERT() << "Unexpected device type.";
    };
  }
}

// Raise an error if the number or sizes of the payloads and the tensors in
// the message do not match the ones that are expected by the ReadOperation.
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
    TP_THROW_ASSERT_IF(tensor.buffer.length() != tensorBeingAllocated.length);
  }
}

// Produce a nop object containing a message descriptor using the information
// contained in the WriteOperation: number and sizes of payloads and tensors,
// tensor descriptors, ...
std::shared_ptr<NopHolder<Packet>> makeDescriptorForMessage(
    const WriteOperation& op) {
  auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
  Packet& nopPacketOut = nopHolderOut->getObject();
  nopPacketOut.Become(nopPacketOut.index_of<MessageDescriptor>());
  MessageDescriptor& nopMessageDescriptor =
      *nopPacketOut.get<MessageDescriptor>();

  nopMessageDescriptor.metadata = op.message.metadata;

  for (int payloadIdx = 0; payloadIdx < op.message.payloads.size();
       ++payloadIdx) {
    const Message::Payload& payload = op.message.payloads[payloadIdx];
    nopMessageDescriptor.payloadDescriptors.emplace_back();
    MessageDescriptor::PayloadDescriptor& nopPayloadDescriptor =
        nopMessageDescriptor.payloadDescriptors.back();
    nopPayloadDescriptor.sizeInBytes = payload.length;
    nopPayloadDescriptor.metadata = payload.metadata;
  }

  TP_DCHECK_EQ(op.message.tensors.size(), op.tensors.size());
  for (int tensorIdx = 0; tensorIdx < op.tensors.size(); ++tensorIdx) {
    const Message::Tensor& tensor = op.message.tensors[tensorIdx];
    const WriteOperation::Tensor& otherTensor = op.tensors[tensorIdx];
    nopMessageDescriptor.tensorDescriptors.emplace_back();
    MessageDescriptor::TensorDescriptor& nopTensorDescriptor =
        nopMessageDescriptor.tensorDescriptors.back();
    nopTensorDescriptor.metadata = tensor.metadata;
    nopTensorDescriptor.channelName = otherTensor.channelName;
    nopTensorDescriptor.deviceType = tensor.buffer.deviceType();
    nopTensorDescriptor.sizeInBytes = tensor.buffer.length();
  }

  return nopHolderOut;
}

struct SelectedTransport {
  std::string name;
  std::string address;
  std::string domainDescriptor;
};

SelectedTransport selectTransport(
    const ContextImpl::TOrderedTransports& orderedTransports,
    const std::unordered_map<std::string, TransportAdvertisement>&
        nopTransportAdvertisement,
    const std::map<std::string, std::string>& addresses) {
  for (const auto& transportContextIter : orderedTransports) {
    const std::string& transportName = std::get<0>(transportContextIter.second);
    const transport::Context& transportContext =
        *(std::get<1>(transportContextIter.second));

    // This pipe's listener might not have an address for that transport.
    const auto addressIter = addresses.find(transportName);
    if (addressIter == addresses.cend()) {
      continue;
    }
    const auto& address = addressIter->second;

    const auto nopTransportAdvertisementIter =
        nopTransportAdvertisement.find(transportName);
    if (nopTransportAdvertisementIter == nopTransportAdvertisement.cend()) {
      continue;
    }
    const TransportAdvertisement& nopCurrentTransportAdvertisement =
        nopTransportAdvertisementIter->second;
    const std::string& domainDescriptor =
        nopCurrentTransportAdvertisement.domainDescriptor;
    if (!transportContext.canCommunicateWithRemote(domainDescriptor)) {
      continue;
    }

    return {transportName, address, transportContext.domainDescriptor()};
  }

  TP_THROW_ASSERT() << "Could not find a viable transport";
  // Returning dummy value to silence compiler warning.
  return {};
}

struct SelectedChannel {
  std::string name;
  std::unordered_map<Device, std::string> deviceDescriptors;
};

std::vector<SelectedChannel> selectChannels(
    const ContextImpl::TOrderedChannels& orderedChannels,
    const std::unordered_map<std::string, ChannelAdvertisement>&
        nopChannelAdvertisement) {
  std::vector<SelectedChannel> result;
  for (const auto& channelContextIter : orderedChannels) {
    const std::string& channelName = std::get<0>(channelContextIter.second);
    const channel::Context& channelContext =
        *(std::get<1>(channelContextIter.second));

    const auto nopChannelAdvertisementIter =
        nopChannelAdvertisement.find(channelName);
    if (nopChannelAdvertisementIter == nopChannelAdvertisement.cend()) {
      continue;
    }
    const ChannelAdvertisement& nopCurrentChannelAdvertisement =
        nopChannelAdvertisementIter->second;
    const std::unordered_map<Device, std::string>& localDeviceDescriptors =
        channelContext.deviceDescriptors();
    const std::unordered_map<Device, std::string>& remoteDeviceDescriptors =
        nopCurrentChannelAdvertisement.deviceDescriptors;

    // Do not select channels which cannot connect anything.
    if (localDeviceDescriptors.empty() || remoteDeviceDescriptors.empty()) {
      continue;
    }

    // For now, only select a channel if it is supported by all pairs of
    // relevant devices. This will be lifted once we introduce per-device pair
    // channel selection.
    bool selected = true;
    for (const auto& localDescIter : localDeviceDescriptors) {
      const std::string& localDeviceDescriptor = localDescIter.second;
      for (const auto& remoteDescIter : remoteDeviceDescriptors) {
        const std::string& remoteDeviceDescriptor = remoteDescIter.second;
        if (!channelContext.canCommunicateWithRemote(
                localDeviceDescriptor, remoteDeviceDescriptor)) {
          selected = false;
        }
      }
    }

    if (selected) {
      result.push_back({channelName, localDeviceDescriptors});
    }
  }

  return result;
}

} // namespace

//
// Initialization
//

PipeImpl::PipeImpl(
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string remoteName,
    const std::string& url)
    : state_(CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE),
      context_(std::move(context)),
      id_(std::move(id)),
      remoteName_(std::move(remoteName)) {
  std::string address;
  std::tie(transport_, address) = splitSchemeOfURL(url);
  connection_ = context_->getTransport(transport_)->connect(std::move(address));
  connection_->setId(id_ + ".tr_" + transport_);
}

PipeImpl::PipeImpl(
    std::shared_ptr<ContextImpl> context,
    std::shared_ptr<ListenerImpl> listener,
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
      connection_(std::move(connection)) {
  connection_->setId(id_ + ".tr_" + transport_);
}

void PipeImpl::init() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->initFromLoop(); });
}

void PipeImpl::initFromLoop() {
  TP_DCHECK(context_->inLoop());

  if (context_->closed()) {
    // Set the error without calling setError because we do not want to invoke
    // handleError as it would find itself in a weird state (since the rest of
    // initFromLoop wouldn't have been called).
    error_ = TP_CREATE_ERROR(PipeClosedError);
    TP_VLOG(1) << "Pipe " << id_ << " is closing (without initing)";
    return;
  }

  context_->enroll(*this);

  if (state_ == CLIENT_ABOUT_TO_SEND_HELLO_AND_BROCHURE) {
    auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
    Packet& nopPacketOut = nopHolderOut->getObject();
    nopPacketOut.Become(nopPacketOut.index_of<SpontaneousConnection>());
    SpontaneousConnection& nopSpontaneousConnection =
        *nopPacketOut.get<SpontaneousConnection>();
    nopSpontaneousConnection.contextName = context_->getName();
    TP_VLOG(3) << "Pipe " << id_
               << " is writing nop object (spontaneous connection)";
    connection_->write(
        *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing nop object (spontaneous connection)";
        }));

    auto nopHolderOut2 = std::make_shared<NopHolder<Packet>>();
    Packet& nopPacketOut2 = nopHolderOut2->getObject();
    nopPacketOut2.Become(nopPacketOut2.index_of<Brochure>());
    Brochure& nopBrochure = *nopPacketOut2.get<Brochure>();
    for (const auto& transportContextIter : context_->getOrderedTransports()) {
      const std::string& transportName =
          std::get<0>(transportContextIter.second);
      const transport::Context& transportContext =
          *(std::get<1>(transportContextIter.second));
      TransportAdvertisement& nopTransportAdvertisement =
          nopBrochure.transportAdvertisement[transportName];
      nopTransportAdvertisement.domainDescriptor =
          transportContext.domainDescriptor();
    }
    for (const auto& channelContextIter : context_->getOrderedChannels()) {
      const std::string& channelName = std::get<0>(channelContextIter.second);
      const channel::Context& channelContext =
          *(std::get<1>(channelContextIter.second));
      nopBrochure.channelAdvertisement[channelName].deviceDescriptors =
          channelContext.deviceDescriptors();
    }
    TP_VLOG(3) << "Pipe " << id_ << " is writing nop object (brochure)";
    connection_->write(
        *nopHolderOut2, callbackWrapper_([nopHolderOut2](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing nop object (brochure)";
        }));
    state_ = CLIENT_WAITING_FOR_BROCHURE_ANSWER;
    auto nopHolderIn = std::make_shared<NopHolder<Packet>>();
    TP_VLOG(3) << "Pipe " << id_ << " is reading nop object (brochure answer)";
    connection_->read(
        *nopHolderIn, callbackWrapper_([nopHolderIn](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done reading nop object (brochure answer)";
          if (!impl.error_) {
            impl.onReadWhileClientWaitingForBrochureAnswer(
                nopHolderIn->getObject());
          }
        }));
  }
  if (state_ == SERVER_WAITING_FOR_BROCHURE) {
    auto nopHolderIn = std::make_shared<NopHolder<Packet>>();
    TP_VLOG(3) << "Pipe " << id_ << " is reading nop object (brochure)";
    connection_->read(
        *nopHolderIn, callbackWrapper_([nopHolderIn](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done reading nop object (brochure)";
          if (!impl.error_) {
            impl.onReadWhileServerWaitingForBrochure(nopHolderIn->getObject());
          }
        }));
  }
}

const std::string& PipeImpl::getRemoteName() {
  return remoteName_;
}

void PipeImpl::close() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->closeFromLoop(); });
}

void PipeImpl::closeFromLoop() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(1) << "Pipe " << id_ << " is closing";
  setError(TP_CREATE_ERROR(PipeClosedError));
}

//
// Entry points for user code
//

void PipeImpl::readDescriptor(read_descriptor_callback_fn fn) {
  context_->deferToLoop(
      [impl{this->shared_from_this()}, fn{std::move(fn)}]() mutable {
        impl->readDescriptorFromLoop(std::move(fn));
      });
}

void PipeImpl::readDescriptorFromLoop(read_descriptor_callback_fn fn) {
  TP_DCHECK(context_->inLoop());

  ReadOpIter opIter = readOps_.emplaceBack(nextMessageBeingRead_++);
  ReadOperation& op = *opIter;

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

  readOps_.advanceOperation(opIter);
}

void PipeImpl::read(Message message, read_callback_fn fn) {
  // Messages aren't copyable and thus if a lambda captures them it cannot be
  // wrapped in a std::function. Therefore we wrap Messages in shared_ptrs.
  auto sharedMessage = std::make_shared<Message>(std::move(message));
  context_->deferToLoop([impl{this->shared_from_this()},
                         sharedMessage{std::move(sharedMessage)},
                         fn{std::move(fn)}]() mutable {
    impl->readFromLoop(std::move(*sharedMessage), std::move(fn));
  });
}

void PipeImpl::readFromLoop(Message message, read_callback_fn fn) {
  TP_DCHECK(context_->inLoop());

  // This is such a bad logical error on the user's side that it doesn't deserve
  // to pass through the channel for "expected errors" (i.e., the callback).
  // This check fails when there is no message for which we are expecting an
  // allocation.
  TP_THROW_ASSERT_IF(!nextMessageGettingAllocation_.has_value());
  ReadOpIter opIter = nextMessageGettingAllocation_.value();
  ReadOperation& op = *opIter;
  nextMessageGettingAllocation_.reset();

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

  op.message = std::move(message);
  op.readCallback = std::move(fn);
  op.doneGettingAllocation = true;

  TP_VLOG(1) << "Pipe " << id_ << " received a read request (#"
             << op.sequenceNumber << ", containing "
             << op.message.payloads.size() << " payloads and "
             << op.message.tensors.size() << " tensors)";

  readOps_.advanceOperation(opIter);
}

void PipeImpl::readPayloadsAndReceiveTensorsOfMessage(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

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
        callbackWrapper_(
            [opIter, payloadIdx](
                PipeImpl& impl, const void* /* unused */, size_t /* unused */) {
              TP_VLOG(3) << "Pipe " << impl.id_ << " done reading payload #"
                         << opIter->sequenceNumber << "." << payloadIdx;
              impl.onReadOfPayload(opIter);
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
        tensor.buffer, callbackWrapper_([opIter, tensorIdx](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done receiving tensor #"
                     << opIter->sequenceNumber << "." << tensorIdx;
          impl.onRecvOfTensor(opIter);
        }));
    ++op.numTensorsBeingReceived;
  }
}

void PipeImpl::write(Message message, write_callback_fn fn) {
  // Messages aren't copyable and thus if a lambda captures them it cannot be
  // wrapped in a std::function. Therefore we wrap Messages in shared_ptrs.
  auto sharedMessage = std::make_shared<Message>(std::move(message));
  context_->deferToLoop([impl{this->shared_from_this()},
                         sharedMessage{std::move(sharedMessage)},
                         fn{std::move(fn)}]() mutable {
    impl->writeFromLoop(std::move(*sharedMessage), std::move(fn));
  });
}

void PipeImpl::writeFromLoop(Message message, write_callback_fn fn) {
  TP_DCHECK(context_->inLoop());

  WriteOpIter opIter = writeOps_.emplaceBack(nextMessageBeingWritten_++);
  WriteOperation& op = *opIter;

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

  writeOps_.advanceOperation(opIter);
}

//
// Helpers to schedule our callbacks into user code
//

void PipeImpl::callReadDescriptorCallback(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  op.readDescriptorCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.readDescriptorCallback = nullptr;
}

void PipeImpl::callReadCallback(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  op.readCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.readCallback = nullptr;
}

void PipeImpl::callWriteCallback(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  op.writeCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.writeCallback = nullptr;
}

//
// Error handling
//

void PipeImpl::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void PipeImpl::handleError() {
  TP_DCHECK(context_->inLoop());
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
    for (const auto& token : iter.second) {
      listener_->unregisterConnectionRequest(token);
    }
  }
  channelRegistrationIds_.clear();
  channelReceivedConnections_.clear();

  readOps_.advanceAllOperations();
  writeOps_.advanceAllOperations();

  context_->unenroll(*this);
}

//
// Everything else
//

void PipeImpl::startReadingUponEstablishingPipe() {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  readOps_.advanceAllOperations();
}

void PipeImpl::startWritingUponEstablishingPipe() {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  writeOps_.advanceAllOperations();
}

void PipeImpl::advanceReadOperation(
    ReadOpIter opIter,
    ReadOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::UNINITIALIZED,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*cond=*/error_ && prevOpState >= ReadOperation::ASKING_FOR_ALLOCATION,
      /*actions=*/{&PipeImpl::callReadDescriptorCallback});

  // The ordering on the "wire" (the primary connection) is descriptor of op N,
  // then payloads of op N, then descriptor of op N+1. Hence this transition
  // must happen after the previous op scheduled its payload read, not just its
  // descriptor read.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::UNINITIALIZED,
      /*to=*/ReadOperation::READING_DESCRIPTOR,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >= ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*actions=*/{&PipeImpl::readDescriptorOfMessage});

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::READING_DESCRIPTOR,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*cond=*/opIter->doneReadingDescriptor &&
          prevOpState >= ReadOperation::ASKING_FOR_ALLOCATION,
      /*actions=*/{&PipeImpl::callReadDescriptorCallback});

  // Needs to wait for previous op to have _received_ the read call, as we can
  // only have exactly one operation at a time for which we expect a read call.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*cond=*/opIter->doneReadingDescriptor &&
          prevOpState >= ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*actions=*/{&PipeImpl::expectReadCall});

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/error_ && opIter->doneGettingAllocation &&
          prevOpState >= ReadOperation::FINISHED,
      /*actions=*/{&PipeImpl::callReadCallback});

  // No need to order this with the previous operation, since all it needs is
  // to come after this own op's descriptor read.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*to=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*cond=*/!error_ && opIter->doneGettingAllocation,
      /*actions=*/{&PipeImpl::readPayloadsAndReceiveTensorsOfMessage});

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/opIter->numPayloadsBeingRead == 0 &&
          opIter->numTensorsBeingReceived == 0 &&
          prevOpState >= ReadOperation::FINISHED,
      /*actions=*/{&PipeImpl::callReadCallback});
}

void PipeImpl::advanceWriteOperation(
    WriteOpIter opIter,
    WriteOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_ && prevOpState >= WriteOperation::FINISHED,
      /*actions=*/{&PipeImpl::callWriteCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of send calls on channels.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::SENDING_TENSORS,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >= WriteOperation::SENDING_TENSORS,
      /*actions=*/{&PipeImpl::sendTensorsOfMessage});

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::SENDING_TENSORS,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_ && opIter->numTensorsBeingSent == 0 &&
          prevOpState >= WriteOperation::FINISHED,
      /*actions=*/{&PipeImpl::callWriteCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the connection.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::SENDING_TENSORS,
      /*to=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*cond=*/!error_ &&
          prevOpState >= WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*actions=*/{&PipeImpl::writeDescriptorAndPayloadsOfMessage});

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/opIter->numPayloadsBeingWritten == 0 &&
          opIter->numTensorsBeingSent == 0 &&
          prevOpState >= WriteOperation::FINISHED,
      /*actions=*/{&PipeImpl::callWriteCallback});
}

void PipeImpl::readDescriptorOfMessage(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_VLOG(2) << "Pipe " << id_ << " is reading descriptor of message #"
             << op.sequenceNumber;

  TP_DCHECK_EQ(connectionState_, AWAITING_DESCRIPTOR);
  TP_DCHECK_EQ(messageBeingReadFromConnection_, op.sequenceNumber);
  auto nopHolderIn = std::make_shared<NopHolder<Packet>>();
  TP_VLOG(3) << "Pipe " << id_ << " is reading nop object (message descriptor #"
             << op.sequenceNumber << ")";
  connection_->read(
      *nopHolderIn, callbackWrapper_([opIter, nopHolderIn](PipeImpl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done reading nop object (message descriptor #"
                   << opIter->sequenceNumber << ")";
        impl.onReadOfMessageDescriptor(opIter, nopHolderIn->getObject());
      }));
  connectionState_ = AWAITING_PAYLOADS;
}

void PipeImpl::expectReadCall(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_DCHECK(!nextMessageGettingAllocation_.has_value());
  nextMessageGettingAllocation_ = opIter;
}

void PipeImpl::sendTensorsOfMessage(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

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

      auto& channelContext = std::get<1>(channelContextIter.second);
      // FIXME: Skip channel if it does not support current buffer type, as a
      // temporary measure before we roll out proper channel selection for
      // cross-device type transfers.
      if (!channelContext->supportsDeviceType(tensor.buffer.deviceType())) {
        continue;
      }

      auto& channel = *(channelIter->second);

      TP_VLOG(3) << "Pipe " << id_ << " is sending tensor #"
                 << op.sequenceNumber << "." << tensorIdx;

      channel.send(
          tensor.buffer, callbackWrapper_([opIter, tensorIdx](PipeImpl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_ << " done sending tensor #"
                       << opIter->sequenceNumber << "." << tensorIdx;
            impl.onSendOfTensor(opIter);
          }));

      op.tensors.push_back(
          WriteOperation::Tensor{tensor.buffer.deviceType(), channelName});

      foundAChannel = true;
      break;
    }
    TP_THROW_ASSERT_IF(!foundAChannel);

    ++op.numTensorsBeingSent;
  }
}

void PipeImpl::writeDescriptorAndPayloadsOfMessage(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  TP_VLOG(2) << "Pipe " << id_
             << " is writing descriptor and payloads of message #"
             << op.sequenceNumber;

  std::shared_ptr<NopHolder<Packet>> holder = makeDescriptorForMessage(op);

  TP_VLOG(3) << "Pipe " << id_ << " is writing nop object (message descriptor #"
             << op.sequenceNumber << ")";
  connection_->write(
      *holder,
      callbackWrapper_(
          [sequenceNumber{op.sequenceNumber}, holder](PipeImpl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_
                       << " done writing nop object (message descriptor #"
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
        callbackWrapper_([opIter, payloadIdx](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done writing payload #"
                     << opIter->sequenceNumber << "." << payloadIdx;
          impl.onWriteOfPayload(opIter);
        }));
    ++op.numPayloadsBeingWritten;
  }
}

void PipeImpl::onReadWhileServerWaitingForBrochure(const Packet& nopPacketIn) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_BROCHURE);
  TP_DCHECK_EQ(nopPacketIn.index(), nopPacketIn.index_of<Brochure>());
  const Brochure& nopBrochure = *nopPacketIn.get<Brochure>();

  auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
  Packet& nopPacketOut = nopHolderOut->getObject();
  nopPacketOut.Become(nopPacketOut.index_of<BrochureAnswer>());
  BrochureAnswer& nopBrochureAnswer = *nopPacketOut.get<BrochureAnswer>();

  auto transport = selectTransport(
      context_->getOrderedTransports(),
      nopBrochure.transportAdvertisement,
      listener_->addresses());

  if (transport.name != transport_) {
    transport_ = transport.name;
    nopBrochureAnswer.transportRegistrationId = registerTransport();
  }

  nopBrochureAnswer.transport = transport.name;
  nopBrochureAnswer.address = transport.address;
  nopBrochureAnswer.transportDomainDescriptor = transport.domainDescriptor;

  const auto selectedChannels = selectChannels(
      context_->getOrderedChannels(), nopBrochure.channelAdvertisement);
  for (const auto& channel : selectedChannels) {
    ChannelSelection& nopChannelSelection =
        nopBrochureAnswer.channelSelection[channel.name];
    nopChannelSelection.registrationIds = registerChannel(channel.name);
    nopChannelSelection.deviceDescriptors = channel.deviceDescriptors;
  }

  TP_VLOG(3) << "Pipe " << id_ << " is writing nop object (brochure answer)";
  connection_->write(
      *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done writing nop object (brochure answer)";
      }));

  if (!pendingRegistrations()) {
    state_ = ESTABLISHED;
    startReadingUponEstablishingPipe();
    startWritingUponEstablishingPipe();
  } else {
    state_ = SERVER_WAITING_FOR_CONNECTIONS;
  }
}

uint64_t PipeImpl::registerTransport() {
  TP_DCHECK(!registrationId_.has_value());
  TP_VLOG(3) << "Pipe " << id_ << " is requesting connection (as replacement)";
  uint64_t token = listener_->registerConnectionRequest(
      callbackWrapper_([](PipeImpl& impl,
                          std::string transport,
                          std::shared_ptr<transport::Connection> connection) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done requesting connection (as replacement)";
        if (!impl.error_) {
          impl.onAcceptWhileServerWaitingForConnection(
              std::move(transport), std::move(connection));
        }
      }));
  registrationId_.emplace(token);

  return token;
}

std::vector<uint64_t>& PipeImpl::registerChannel(
    const std::string& channelName) {
  const channel::Context& channelContext = *context_->getChannel(channelName);
  const size_t numConnectionsNeeded = channelContext.numConnectionsNeeded();
  auto& channelRegistrationIds = channelRegistrationIds_[channelName];
  channelRegistrationIds.resize(numConnectionsNeeded);
  auto& channelReceivedConnections = channelReceivedConnections_[channelName];
  channelReceivedConnections.resize(numConnectionsNeeded);
  for (size_t connId = 0; connId < numConnectionsNeeded; ++connId) {
    TP_VLOG(3) << "Pipe " << id_ << " is requesting connection " << connId
               << "/" << numConnectionsNeeded << " (for channel " << channelName
               << ")";
    uint64_t token = listener_->registerConnectionRequest(callbackWrapper_(
        [channelName, connId, numConnectionsNeeded](
            PipeImpl& impl,
            std::string transport,
            std::shared_ptr<transport::Connection> connection) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done requesting connection "
                     << connId << "/" << numConnectionsNeeded
                     << " (for channel " << channelName << ")";
          if (!impl.error_) {
            impl.onAcceptWhileServerWaitingForChannel(
                channelName,
                connId,
                std::move(transport),
                std::move(connection));
          }
        }));
    channelRegistrationIds[connId] = token;
  }

  return channelRegistrationIds;
}

void PipeImpl::onReadWhileClientWaitingForBrochureAnswer(
    const Packet& nopPacketIn) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, CLIENT_WAITING_FOR_BROCHURE_ANSWER);
  TP_DCHECK_EQ(nopPacketIn.index(), nopPacketIn.index_of<BrochureAnswer>());

  const BrochureAnswer& nopBrochureAnswer = *nopPacketIn.get<BrochureAnswer>();
  const std::string& transport = nopBrochureAnswer.transport;
  std::string address = nopBrochureAnswer.address;
  std::shared_ptr<transport::Context> transportContext =
      context_->getTransport(transport);
  TP_DCHECK(transportContext->canCommunicateWithRemote(
      nopBrochureAnswer.transportDomainDescriptor))
      << "The two endpoints disagree on whether transport " << transport
      << " can be used to communicate";

  if (transport != transport_) {
    TP_VLOG(3) << "Pipe " << id_ << " is opening connection (as replacement)";
    std::shared_ptr<transport::Connection> connection =
        transportContext->connect(address);
    connection->setId(id_ + ".tr_" + transport);
    auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
    Packet& nopPacketOut = nopHolderOut->getObject();
    nopPacketOut.Become(nopPacketOut.index_of<RequestedConnection>());
    RequestedConnection& nopRequestedConnection =
        *nopPacketOut.get<RequestedConnection>();
    uint64_t token = nopBrochureAnswer.transportRegistrationId;
    nopRequestedConnection.registrationId = token;
    TP_VLOG(3) << "Pipe " << id_
               << " is writing nop object (requested connection)";
    connection->write(
        *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing nop object (requested connection)";
        }));

    transport_ = transport;
    connection_ = std::move(connection);
  }

  for (const auto& nopChannelSelectionIter :
       nopBrochureAnswer.channelSelection) {
    const std::string& channelName = nopChannelSelectionIter.first;
    const ChannelSelection& nopChannelSelection =
        nopChannelSelectionIter.second;

    std::shared_ptr<channel::Context> channelContext =
        context_->getChannel(channelName);

    for (const auto& localDescIter : channelContext->deviceDescriptors()) {
      const std::string& localDeviceDescriptor = localDescIter.second;
      for (const auto& remoteDescIter : nopChannelSelection.deviceDescriptors) {
        const std::string& remoteDeviceDescriptor = remoteDescIter.second;
        TP_DCHECK(channelContext->canCommunicateWithRemote(
            localDeviceDescriptor, remoteDeviceDescriptor))
            << "The two endpoints disagree on whether channel " << channelName
            << " can be used to communicate";
      }
    }

    const size_t numConnectionsNeeded = channelContext->numConnectionsNeeded();
    TP_DCHECK_EQ(
        numConnectionsNeeded, nopChannelSelection.registrationIds.size());
    std::vector<std::shared_ptr<transport::Connection>> connections(
        numConnectionsNeeded);
    for (size_t connId = 0; connId < numConnectionsNeeded; ++connId) {
      TP_VLOG(3) << "Pipe " << id_ << " is opening connection " << connId << "/"
                 << numConnectionsNeeded << " (for channel " << channelName
                 << ")";
      std::shared_ptr<transport::Connection> connection =
          transportContext->connect(address);
      connection->setId(
          id_ + ".ch_" + channelName + "_" + std::to_string(connId));

      auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
      Packet& nopPacketOut = nopHolderOut->getObject();
      nopPacketOut.Become(nopPacketOut.index_of<RequestedConnection>());
      RequestedConnection& nopRequestedConnection =
          *nopPacketOut.get<RequestedConnection>();
      uint64_t token = nopChannelSelection.registrationIds[connId];
      nopRequestedConnection.registrationId = token;
      TP_VLOG(3) << "Pipe " << id_
                 << " is writing nop object (requested connection)";
      connection->write(
          *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_
                       << " done writing nop object (requested connection)";
          }));
      connections[connId] = std::move(connection);
    }

    std::shared_ptr<channel::Channel> channel = channelContext->createChannel(
        std::move(connections), channel::Endpoint::kConnect);
    channel->setId(id_ + ".ch_" + channelName);
    channels_.emplace(channelName, std::move(channel));
  }

  state_ = ESTABLISHED;
  startReadingUponEstablishingPipe();
  startWritingUponEstablishingPipe();
}

void PipeImpl::onAcceptWhileServerWaitingForConnection(
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  TP_DCHECK(registrationId_.has_value());
  listener_->unregisterConnectionRequest(registrationId_.value());
  registrationId_.reset();
  receivedConnection->setId(id_ + ".tr_" + receivedTransport);
  TP_DCHECK_EQ(transport_, receivedTransport);
  connection_.reset();
  connection_ = std::move(receivedConnection);

  if (!pendingRegistrations()) {
    state_ = ESTABLISHED;
    startReadingUponEstablishingPipe();
    startWritingUponEstablishingPipe();
  }
}

void PipeImpl::onAcceptWhileServerWaitingForChannel(
    std::string channelName,
    size_t connId,
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  TP_DCHECK_EQ(transport_, receivedTransport);
  auto channelRegistrationIdsIter = channelRegistrationIds_.find(channelName);
  TP_DCHECK(channelRegistrationIdsIter != channelRegistrationIds_.end());
  listener_->unregisterConnectionRequest(
      channelRegistrationIdsIter->second[connId]);
  receivedConnection->setId(
      id_ + ".ch_" + channelName + "_" + std::to_string(connId));

  channelReceivedConnections_[channelName][connId] =
      std::move(receivedConnection);
  // TODO: If we can guarantee the order in which the accept() calls happen,
  // this check can be replaced with `if (connId == numConnectionsNeeded -
  // 1)`.
  for (const auto& conn : channelReceivedConnections_[channelName]) {
    if (conn == nullptr) {
      return;
    }
  }

  std::shared_ptr<channel::Context> channelContext =
      context_->getChannel(channelName);

  std::shared_ptr<channel::Channel> channel = channelContext->createChannel(
      std::move(channelReceivedConnections_[channelName]),
      channel::Endpoint::kListen);
  channel->setId(id_ + ".ch_" + channelName);

  channelRegistrationIds_.erase(channelRegistrationIdsIter);
  channelReceivedConnections_.erase(channelName);

  TP_DCHECK(channels_.find(channelName) == channels_.end());
  channels_.emplace(channelName, std::move(channel));

  if (!pendingRegistrations()) {
    state_ = ESTABLISHED;
    startReadingUponEstablishingPipe();
    startWritingUponEstablishingPipe();
  }
}

void PipeImpl::onReadOfMessageDescriptor(
    ReadOpIter opIter,
    const Packet& nopPacketIn) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  op.doneReadingDescriptor = true;
  if (!error_) {
    parseDescriptorOfMessage(op, nopPacketIn);
  }

  readOps_.advanceOperation(opIter);
}

void PipeImpl::onReadOfPayload(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  op.numPayloadsBeingRead--;

  readOps_.advanceOperation(opIter);
}

void PipeImpl::onRecvOfTensor(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  op.numTensorsBeingReceived--;

  readOps_.advanceOperation(opIter);
}

void PipeImpl::onWriteOfPayload(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  op.numPayloadsBeingWritten--;

  writeOps_.advanceOperation(opIter);
}

void PipeImpl::onSendOfTensor(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  op.numTensorsBeingSent--;

  writeOps_.advanceOperation(opIter);
}

bool PipeImpl::pendingRegistrations() {
  if (registrationId_.has_value()) {
    return true;
  }

  if (!channelRegistrationIds_.empty()) {
    return true;
  }

  return false;
}

} // namespace tensorpipe
