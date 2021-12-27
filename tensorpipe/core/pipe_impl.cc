/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

namespace tensorpipe {

namespace {

void parseDescriptorReplyOfMessage(
    WriteOperation& op,
    DescriptorReply nopDescriptorReply) {
  const int numTensors = op.message.tensors.size();
  size_t targetDeviceIdx = 0;
  for (size_t tensorIdx = 0; tensorIdx < numTensors; ++tensorIdx) {
    const Message::Tensor& tensor = op.message.tensors[tensorIdx];
    WriteOperation::Tensor& tensorBeingSent = op.tensors[tensorIdx];
    if (!tensor.targetDevice.has_value()) {
      tensorBeingSent.targetDevice =
          std::move(nopDescriptorReply.targetDevices[targetDeviceIdx++]);
    }
  }
  TP_DCHECK_EQ(targetDeviceIdx, nopDescriptorReply.targetDevices.size());
}

// Raise an error if the number of payloads and tensors in the allocation do not
// match the ones that are expected by the ReadOperation. Also checks that
// tensors are allocated on the correct devices.
void checkAllocationCompatibility(
    const Descriptor& descriptor,
    const Allocation& allocation) {
  size_t numPayloads = allocation.payloads.size();
  TP_THROW_ASSERT_IF(numPayloads != descriptor.payloads.size());

  size_t numTensors = allocation.tensors.size();
  TP_THROW_ASSERT_IF(numTensors != descriptor.tensors.size());
  for (size_t tensorIdx = 0; tensorIdx < numTensors; tensorIdx++) {
    const Allocation::Tensor& tensor = allocation.tensors[tensorIdx];
    const Descriptor::Tensor& tensorDescriptor = descriptor.tensors[tensorIdx];
    if (tensorDescriptor.targetDevice.has_value()) {
      TP_THROW_ASSERT_IF(
          !(tensor.buffer.device() == tensorDescriptor.targetDevice.value()));
    }
  }
}

// Produce a nop object containing a message descriptor using the information
// contained in the WriteOperation: number and sizes of payloads and tensors,
// tensor descriptors, ...
std::shared_ptr<NopHolder<Descriptor>> makeDescriptorForMessage(
    const WriteOperation& op) {
  auto nopHolderOut = std::make_shared<NopHolder<Descriptor>>();
  Descriptor& nopDescriptor = nopHolderOut->getObject();

  nopDescriptor.metadata = op.message.metadata;

  for (int payloadIdx = 0; payloadIdx < op.message.payloads.size();
       ++payloadIdx) {
    const Message::Payload& payload = op.message.payloads[payloadIdx];
    nopDescriptor.payloads.emplace_back();
    Descriptor::Payload& nopPayloadDescriptor = nopDescriptor.payloads.back();
    nopPayloadDescriptor.length = payload.length;
    nopPayloadDescriptor.metadata = payload.metadata;
  }

  TP_DCHECK_EQ(op.message.tensors.size(), op.tensors.size());
  for (int tensorIdx = 0; tensorIdx < op.tensors.size(); ++tensorIdx) {
    const Message::Tensor& tensor = op.message.tensors[tensorIdx];
    nopDescriptor.tensors.emplace_back();
    Descriptor::Tensor& nopTensorDescriptor = nopDescriptor.tensors.back();
    nopTensorDescriptor.metadata = tensor.metadata;
    nopTensorDescriptor.sourceDevice = tensor.buffer.device();
    if (tensor.targetDevice.has_value()) {
      nopTensorDescriptor.targetDevice = tensor.targetDevice.value();
    }
    nopTensorDescriptor.length = tensor.length;
  }

  return nopHolderOut;
}

std::shared_ptr<NopHolder<DescriptorReply>> makeDescriptorReplyForMessage(
    const ReadOperation& op) {
  auto nopHolderOut = std::make_shared<NopHolder<DescriptorReply>>();
  DescriptorReply& nopDescriptorReply = nopHolderOut->getObject();

  for (size_t tensorIdx = 0; tensorIdx < op.descriptor.tensors.size();
       ++tensorIdx) {
    if (!op.descriptor.tensors[tensorIdx].targetDevice.has_value()) {
      const Allocation::Tensor& tensor = op.allocation.tensors[tensorIdx];
      nopDescriptorReply.targetDevices.push_back(tensor.buffer.device());
    }
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
    const std::unordered_map<std::string, std::string>& remoteDomainDescriptors,
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

    const auto remoteDomainDescriptorsIter =
        remoteDomainDescriptors.find(transportName);
    if (remoteDomainDescriptorsIter == remoteDomainDescriptors.cend()) {
      continue;
    }
    const std::string& remoteDomainDescriptor =
        remoteDomainDescriptorsIter->second;
    if (!transportContext.canCommunicateWithRemote(remoteDomainDescriptor)) {
      continue;
    }

    return {transportName, address, transportContext.domainDescriptor()};
  }

  TP_THROW_ASSERT() << "Could not find a viable transport";
  // Returning dummy value to silence compiler warning.
  return {};
}

struct SelectedChannels {
  std::unordered_map<std::string, std::unordered_map<Device, std::string>>
      descriptorsMap;
  std::unordered_map<std::pair<Device, Device>, std::string>
      channelForDevicePair;
};

SelectedChannels selectChannels(
    const ContextImpl::TOrderedChannels& orderedChannels,
    const std::unordered_map<
        std::string,
        std::unordered_map<Device, std::string>>& remoteDescriptorsMap) {
  SelectedChannels result;

  for (const auto& channelIter : orderedChannels) {
    const std::string& channelName = std::get<0>(channelIter.second);
    const channel::Context& channelContext = *std::get<1>(channelIter.second);

    const auto& remoteDescriptorsMapIter =
        remoteDescriptorsMap.find(channelName);
    if (remoteDescriptorsMapIter == remoteDescriptorsMap.end()) {
      continue;
    }

    const std::unordered_map<Device, std::string>& localDeviceDescriptors =
        channelContext.deviceDescriptors();
    const std::unordered_map<Device, std::string>& remoteDeviceDescriptors =
        remoteDescriptorsMapIter->second;

    bool selected = false;
    for (const auto& localDescIter : localDeviceDescriptors) {
      const Device& localDevice = localDescIter.first;
      const std::string& localDeviceDescriptor = localDescIter.second;
      for (const auto& remoteDescIter : remoteDeviceDescriptors) {
        const Device& remoteDevice = remoteDescIter.first;
        const std::string& remoteDeviceDescriptor = remoteDescIter.second;

        if (!channelContext.canCommunicateWithRemote(
                localDeviceDescriptor, remoteDeviceDescriptor)) {
          continue;
        }

        if (result.channelForDevicePair.count({localDevice, remoteDevice}) !=
            0) {
          // A channel with higher priority has already been selected for this
          // device pair.
          continue;
        }

        selected = true;
        result.channelForDevicePair[{localDevice, remoteDevice}] = channelName;
      }
    }

    if (selected) {
      result.descriptorsMap[channelName] = localDeviceDescriptors;
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
  descriptorConnection_ =
      context_->getTransport(transport_)->connect(std::move(address));
  descriptorConnection_->setId(id_ + ".d.tr_" + transport_);
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
      descriptorConnection_(std::move(connection)) {
  descriptorConnection_->setId(id_ + ".d.tr_" + transport_);
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
    descriptorConnection_->write(
        *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing nop object (spontaneous connection)";
        }));

    auto nopHolderOut2 = std::make_shared<NopHolder<Brochure>>();
    Brochure& nopBrochure = nopHolderOut2->getObject();
    for (const auto& transportContextIter : context_->getOrderedTransports()) {
      const std::string& transportName =
          std::get<0>(transportContextIter.second);
      const transport::Context& transportContext =
          *(std::get<1>(transportContextIter.second));
      nopBrochure.transportDomainDescriptors[transportName] =
          transportContext.domainDescriptor();
    }
    for (const auto& channelContextIter : context_->getOrderedChannels()) {
      const std::string& channelName = std::get<0>(channelContextIter.second);
      const channel::Context& channelContext =
          *(std::get<1>(channelContextIter.second));
      nopBrochure.channelDeviceDescriptors[channelName] =
          channelContext.deviceDescriptors();
    }
    TP_VLOG(3) << "Pipe " << id_ << " is writing nop object (brochure)";
    descriptorConnection_->write(
        *nopHolderOut2, callbackWrapper_([nopHolderOut2](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_
                     << " done writing nop object (brochure)";
        }));
    state_ = CLIENT_WAITING_FOR_BROCHURE_ANSWER;
    auto nopHolderIn = std::make_shared<NopHolder<BrochureAnswer>>();
    TP_VLOG(3) << "Pipe " << id_ << " is reading nop object (brochure answer)";
    descriptorConnection_->read(
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
    auto nopHolderIn = std::make_shared<NopHolder<Brochure>>();
    TP_VLOG(3) << "Pipe " << id_ << " is reading nop object (brochure)";
    descriptorConnection_->read(
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
           const Error& error, Descriptor descriptor) {
    TP_DCHECK_EQ(sequenceNumber, nextReadDescriptorCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a readDescriptor callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(descriptor));
    TP_VLOG(1) << "Pipe " << id_ << " done calling a readDescriptor callback (#"
               << sequenceNumber << ")";
  };

  op.readDescriptorCallback = std::move(fn);

  readOps_.advanceOperation(opIter);
}

void PipeImpl::read(Allocation allocation, read_callback_fn fn) {
  context_->deferToLoop([impl{this->shared_from_this()},
                         allocation{std::move(allocation)},
                         fn{std::move(fn)}]() mutable {
    impl->readFromLoop(std::move(allocation), std::move(fn));
  });
}

void PipeImpl::readFromLoop(Allocation allocation, read_callback_fn fn) {
  TP_DCHECK(context_->inLoop());

  // This is such a bad logical error on the user's side that it doesn't deserve
  // to pass through the channel for "expected errors" (i.e., the callback).
  // This check fails when there is no message for which we are expecting an
  // allocation.
  TP_THROW_ASSERT_IF(!nextMessageGettingAllocation_.has_value());
  ReadOpIter opIter = nextMessageGettingAllocation_.value();
  ReadOperation& op = *opIter;
  nextMessageGettingAllocation_.reset();

  checkAllocationCompatibility(op.descriptor, allocation);

  fn = [this, sequenceNumber{op.sequenceNumber}, fn{std::move(fn)}](
           const Error& error) {
    TP_DCHECK_EQ(sequenceNumber, nextReadCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a read callback (#"
               << sequenceNumber << ")";
    fn(error);
    TP_VLOG(1) << "Pipe " << id_ << " done calling a read callback (#"
               << sequenceNumber << ")";
  };

  op.allocation = std::move(allocation);
  op.readCallback = std::move(fn);
  op.doneGettingAllocation = true;

  TP_VLOG(1) << "Pipe " << id_ << " received a read request (#"
             << op.sequenceNumber << ", containing "
             << op.allocation.payloads.size() << " payloads and "
             << op.allocation.tensors.size() << " tensors)";

  readOps_.advanceOperation(opIter);
}

void PipeImpl::readPayloadsOfMessage(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_VLOG(2) << "Pipe " << id_ << " is reading payloads of message #"
             << op.sequenceNumber;

  TP_DCHECK_EQ(connectionState_, AWAITING_PAYLOADS);
  TP_DCHECK_EQ(messageBeingReadFromConnection_, op.sequenceNumber);
  for (size_t payloadIdx = 0; payloadIdx < op.allocation.payloads.size();
       payloadIdx++) {
    Allocation::Payload& payload = op.allocation.payloads[payloadIdx];
    Descriptor::Payload& payloadDescriptor = op.descriptor.payloads[payloadIdx];
    TP_VLOG(3) << "Pipe " << id_ << " is reading payload #" << op.sequenceNumber
               << "." << payloadIdx;
    descriptorConnection_->read(
        payload.data,
        payloadDescriptor.length,
        callbackWrapper_(
            [opIter, payloadIdx](
                PipeImpl& impl, const void* /* unused */, size_t /* unused */) {
              TP_VLOG(3) << "Pipe " << impl.id_ << " done reading payload #"
                         << opIter->sequenceNumber << "." << payloadIdx;
              opIter->numPayloadsBeingRead--;
              impl.readOps_.advanceOperation(opIter);
            }));
    ++op.numPayloadsBeingRead;
  }
  connectionState_ = AWAITING_DESCRIPTOR;
  ++messageBeingReadFromConnection_;
}

void PipeImpl::receiveTensorsOfMessage(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_VLOG(2) << "Pipe " << id_ << " is receiving tensors of message #"
             << op.sequenceNumber;

  TP_DCHECK_EQ(op.descriptor.tensors.size(), op.allocation.tensors.size());
  for (size_t tensorIdx = 0; tensorIdx < op.descriptor.tensors.size();
       ++tensorIdx) {
    Allocation::Tensor& tensor = op.allocation.tensors[tensorIdx];
    const Descriptor::Tensor& tensorDescriptor =
        op.descriptor.tensors[tensorIdx];

    const Device& localDevice = tensor.buffer.device();
    const Device& remoteDevice = tensorDescriptor.sourceDevice;
    const auto& channelIter =
        channelForDevicePair_.find({localDevice, remoteDevice});
    TP_THROW_ASSERT_IF(channelIter == channelForDevicePair_.end())
        << "Could not find suitable channel for sending from local device "
        << localDevice.toString() << " to remote device "
        << remoteDevice.toString();

    const std::string& channelName = channelIter->second;
    channel::Channel& channel = *channels_.at(channelName);
    TP_VLOG(3) << "Pipe " << id_ << " is receiving tensor #"
               << op.sequenceNumber << "." << tensorIdx;

    channel.recv(
        tensor.buffer,
        tensorDescriptor.length,
        callbackWrapper_([opIter, tensorIdx](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done receiving tensor #"
                     << opIter->sequenceNumber << "." << tensorIdx;
          opIter->numTensorsBeingReceived--;
          impl.readOps_.advanceOperation(opIter);
        }));
    ++op.numTensorsBeingReceived;
  }
}

void PipeImpl::writeDescriptorReplyOfMessage(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_DCHECK(op.hasMissingTargetDevices);

  std::shared_ptr<NopHolder<DescriptorReply>> holder =
      makeDescriptorReplyForMessage(op);

  TP_VLOG(3) << "Pipe " << id_
             << " is writing nop object (message descriptor reply #"
             << op.sequenceNumber << ")";
  descriptorReplyConnection_->write(
      *holder,
      callbackWrapper_(
          [sequenceNumber{op.sequenceNumber}, holder](PipeImpl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_
                       << " done writing nop object (message descriptor reply #"
                       << sequenceNumber << ")";
          }));
}

void PipeImpl::write(Message message, write_callback_fn fn) {
  context_->deferToLoop([impl{this->shared_from_this()},
                         message{std::move(message)},
                         fn{std::move(fn)}]() mutable {
    impl->writeFromLoop(std::move(message), std::move(fn));
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
           const Error& error) {
    TP_DCHECK_EQ(sequenceNumber, nextWriteCallbackToCall_++);
    TP_VLOG(1) << "Pipe " << id_ << " is calling a write callback (#"
               << sequenceNumber << ")";
    fn(error);
    TP_VLOG(1) << "Pipe " << id_ << " done calling a write callback (#"
               << sequenceNumber << ")";
  };

  size_t numTensors = message.tensors.size();
  op.tensors.resize(numTensors);
  for (size_t tensorIdx = 0; tensorIdx < numTensors; ++tensorIdx) {
    const Message::Tensor& tensor = message.tensors[tensorIdx];
    WriteOperation::Tensor& tensorBeingSent = op.tensors[tensorIdx];
    tensorBeingSent.sourceDevice = tensor.buffer.device();
    if (tensor.targetDevice.has_value()) {
      tensorBeingSent.targetDevice = *tensor.targetDevice;
    } else {
      op.hasMissingTargetDevices = true;
    }
  }

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

  op.readDescriptorCallback(error_, op.descriptor);
  // Reset callback to release the resources it was holding.
  op.readDescriptorCallback = nullptr;
}

void PipeImpl::callReadCallback(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  op.readCallback(error_);
  // Reset callback to release the resources it was holding.
  op.readCallback = nullptr;
}

void PipeImpl::callWriteCallback(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  op.writeCallback(error_);
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

  descriptorConnection_->close();

  if (descriptorReplyConnection_) {
    descriptorReplyConnection_->close();
  }

  for (auto& channelIter : channels_) {
    channelIter.second->close();
  }

  for (const auto& tokenIter : registrationIds_) {
    listener_->unregisterConnectionRequest(tokenIter.second);
  }
  registrationIds_.clear();

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

void PipeImpl::advanceReadOperation(
    ReadOpIter opIter,
    ReadOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

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
      /*cond=*/op.doneReadingDescriptor &&
          prevOpState >= ReadOperation::ASKING_FOR_ALLOCATION,
      /*actions=*/{&PipeImpl::callReadDescriptorCallback});

  // Needs to wait for previous op to have _received_ the read call, as we can
  // only have exactly one operation at a time for which we expect a read call.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*cond=*/op.doneReadingDescriptor &&
          prevOpState >= ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*actions=*/{&PipeImpl::expectReadCall});

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/error_ && op.doneGettingAllocation &&
          prevOpState >= ReadOperation::FINISHED,
      /*actions=*/{&PipeImpl::callReadCallback});

  // No need to order this with the previous operation, since all it needs is
  // to come after this own op's descriptor read.
  // This transition shortcuts writing the descriptor reply when all target
  // devices were provided by the sender.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*to=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*cond=*/!error_ && op.doneGettingAllocation &&
          !op.hasMissingTargetDevices,
      /*actions=*/
      {&PipeImpl::readPayloadsOfMessage, &PipeImpl::receiveTensorsOfMessage});

  // No need to order this with the previous operation, since all it needs is
  // to come after this own op's descriptor read.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*to=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*cond=*/!error_ && op.doneGettingAllocation &&
          op.hasMissingTargetDevices,
      /*actions=*/
      {&PipeImpl::readPayloadsOfMessage,
       &PipeImpl::writeDescriptorReplyOfMessage,
       &PipeImpl::receiveTensorsOfMessage});

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/op.numPayloadsBeingRead == 0 &&
          op.numTensorsBeingReceived == 0 &&
          prevOpState >= ReadOperation::FINISHED,
      /*actions=*/{&PipeImpl::callReadCallback});
}

void PipeImpl::advanceWriteOperation(
    WriteOpIter opIter,
    WriteOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_ && prevOpState >= WriteOperation::FINISHED,
      /*actions=*/{&PipeImpl::callWriteCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the connection and send calls on the channels.
  // This transition shortcuts reading the target devices when they were all
  // provided by the user.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          !op.hasMissingTargetDevices &&
          prevOpState >= WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*actions=*/
      {&PipeImpl::writeDescriptorOfMessage,
       &PipeImpl::writePayloadsOfMessage,
       &PipeImpl::sendTensorsOfMessage});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the descriptor connection and read calls on the
  // descriptor reply connection.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::WRITING_PAYLOADS_AND_READING_TARGET_DEVICES,
      /*cond=*/!error_ && state_ == ESTABLISHED && op.hasMissingTargetDevices &&
          prevOpState >=
              WriteOperation::WRITING_PAYLOADS_AND_READING_TARGET_DEVICES,
      /*actions=*/
      {&PipeImpl::writeDescriptorOfMessage,
       &PipeImpl::writePayloadsOfMessage,
       &PipeImpl::readDescriptorReplyOfMessage});

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::WRITING_PAYLOADS_AND_READING_TARGET_DEVICES,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_ && op.numPayloadsBeingWritten == 0 &&
          op.doneReadingDescriptorReply &&
          prevOpState >= WriteOperation::FINISHED,
      /*actions=*/{&PipeImpl::callWriteCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of send calls on channels.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::WRITING_PAYLOADS_AND_READING_TARGET_DEVICES,
      /*to=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*cond=*/!error_ && op.doneReadingDescriptorReply &&
          prevOpState >= WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*actions=*/{&PipeImpl::sendTensorsOfMessage});

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/op.numPayloadsBeingWritten == 0 && op.numTensorsBeingSent == 0 &&
          prevOpState >= WriteOperation::FINISHED,
      /*actions=*/{&PipeImpl::callWriteCallback});
}

void PipeImpl::readDescriptorOfMessage(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_DCHECK_EQ(connectionState_, AWAITING_DESCRIPTOR);
  TP_DCHECK_EQ(messageBeingReadFromConnection_, op.sequenceNumber);
  auto nopHolderIn = std::make_shared<NopHolder<Descriptor>>();
  TP_VLOG(3) << "Pipe " << id_ << " is reading nop object (message descriptor #"
             << op.sequenceNumber << ")";
  descriptorConnection_->read(
      *nopHolderIn, callbackWrapper_([opIter, nopHolderIn](PipeImpl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done reading nop object (message descriptor #"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingDescriptor = true;
        if (!impl.error_) {
          opIter->descriptor = std::move(nopHolderIn->getObject());
          for (const auto& tensor : opIter->descriptor.tensors) {
            if (!tensor.targetDevice.has_value()) {
              opIter->hasMissingTargetDevices = true;
            }
          }
        }
        impl.readOps_.advanceOperation(opIter);
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

  TP_DCHECK_EQ(op.message.tensors.size(), op.tensors.size());
  for (size_t tensorIdx = 0; tensorIdx < op.message.tensors.size();
       ++tensorIdx) {
    const auto& tensor = op.message.tensors[tensorIdx];

    const Device& localDevice = op.tensors[tensorIdx].sourceDevice;
    TP_DCHECK(op.tensors[tensorIdx].targetDevice.has_value());
    const Device& remoteDevice = *op.tensors[tensorIdx].targetDevice;
    const auto& channelIter =
        channelForDevicePair_.find({localDevice, remoteDevice});
    TP_THROW_ASSERT_IF(channelIter == channelForDevicePair_.end())
        << "Could not find suitable channel for sending from local device "
        << localDevice.toString() << " to remote device "
        << remoteDevice.toString();
    const std::string& channelName = channelIter->second;

    channel::Channel& channel = *channels_[channelName];

    TP_VLOG(3) << "Pipe " << id_ << " is sending tensor #" << op.sequenceNumber
               << "." << tensorIdx;

    channel.send(
        tensor.buffer,
        tensor.length,
        callbackWrapper_([opIter, tensorIdx](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done sending tensor #"
                     << opIter->sequenceNumber << "." << tensorIdx;
          opIter->numTensorsBeingSent--;
          impl.writeOps_.advanceOperation(opIter);
        }));

    ++op.numTensorsBeingSent;
  }
}

void PipeImpl::writeDescriptorOfMessage(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  std::shared_ptr<NopHolder<Descriptor>> holder = makeDescriptorForMessage(op);

  TP_VLOG(3) << "Pipe " << id_ << " is writing nop object (message descriptor #"
             << op.sequenceNumber << ")";
  descriptorConnection_->write(
      *holder,
      callbackWrapper_(
          [sequenceNumber{op.sequenceNumber}, holder](PipeImpl& impl) {
            TP_VLOG(3) << "Pipe " << impl.id_
                       << " done writing nop object (message descriptor #"
                       << sequenceNumber << ")";
          }));
}

void PipeImpl::writePayloadsOfMessage(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  TP_VLOG(2) << "Pipe " << id_ << " is writing payloads of message #"
             << op.sequenceNumber;

  for (size_t payloadIdx = 0; payloadIdx < op.message.payloads.size();
       payloadIdx++) {
    Message::Payload& payload = op.message.payloads[payloadIdx];
    TP_VLOG(3) << "Pipe " << id_ << " is writing payload #" << op.sequenceNumber
               << "." << payloadIdx;
    descriptorConnection_->write(
        payload.data,
        payload.length,
        callbackWrapper_([opIter, payloadIdx](PipeImpl& impl) {
          TP_VLOG(3) << "Pipe " << impl.id_ << " done writing payload #"
                     << opIter->sequenceNumber << "." << payloadIdx;
          opIter->numPayloadsBeingWritten--;
          impl.writeOps_.advanceOperation(opIter);
        }));
    ++op.numPayloadsBeingWritten;
  }
}

void PipeImpl::readDescriptorReplyOfMessage(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  TP_DCHECK(op.hasMissingTargetDevices);

  auto nopHolderIn = std::make_shared<NopHolder<DescriptorReply>>();
  TP_VLOG(3) << "Pipe " << id_
             << " is reading nop object (message descriptor reply #"
             << op.sequenceNumber << ")";
  descriptorReplyConnection_->read(
      *nopHolderIn, callbackWrapper_([opIter, nopHolderIn](PipeImpl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done reading nop object (message descriptor reply #"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingDescriptorReply = true;
        if (!impl.error_) {
          parseDescriptorReplyOfMessage(
              *opIter, std::move(nopHolderIn->getObject()));
        }
        impl.writeOps_.advanceOperation(opIter);
      }));
}

void PipeImpl::onReadWhileServerWaitingForBrochure(
    const Brochure& nopBrochure) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_BROCHURE);

  auto nopHolderOut = std::make_shared<NopHolder<BrochureAnswer>>();
  BrochureAnswer& nopBrochureAnswer = nopHolderOut->getObject();

  auto transport = selectTransport(
      context_->getOrderedTransports(),
      nopBrochure.transportDomainDescriptors,
      listener_->addresses());

  if (transport.name != transport_) {
    transport_ = transport.name;
    nopBrochureAnswer.transportRegistrationIds[ConnectionId::DESCRIPTOR] =
        registerTransport(ConnectionId::DESCRIPTOR);
  }
  nopBrochureAnswer.transportRegistrationIds[ConnectionId::DESCRIPTOR_REPLY] =
      registerTransport(ConnectionId::DESCRIPTOR_REPLY);

  nopBrochureAnswer.transport = transport.name;
  nopBrochureAnswer.address = transport.address;
  nopBrochureAnswer.transportDomainDescriptor = transport.domainDescriptor;

  SelectedChannels selectedChannels = selectChannels(
      context_->getOrderedChannels(), nopBrochure.channelDeviceDescriptors);
  channelForDevicePair_ = std::move(selectedChannels.channelForDevicePair);
  nopBrochureAnswer.channelForDevicePair = channelForDevicePair_;

  for (auto& descriptorsIter : selectedChannels.descriptorsMap) {
    const std::string& channelName = descriptorsIter.first;
    nopBrochureAnswer.channelRegistrationIds[channelName] =
        registerChannel(channelName);
    std::unordered_map<Device, std::string>& deviceDescriptors =
        descriptorsIter.second;
    nopBrochureAnswer.channelDeviceDescriptors[channelName] =
        std::move(deviceDescriptors);
  }

  TP_VLOG(3) << "Pipe " << id_ << " is writing nop object (brochure answer)";
  descriptorConnection_->write(
      *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done writing nop object (brochure answer)";
      }));

  if (!pendingRegistrations()) {
    state_ = ESTABLISHED;
    readOps_.advanceAllOperations();
    writeOps_.advanceAllOperations();
  } else {
    state_ = SERVER_WAITING_FOR_CONNECTIONS;
  }
}

uint64_t PipeImpl::registerTransport(ConnectionId connId) {
  TP_DCHECK(registrationIds_.count(connId) == 0);
  TP_VLOG(3) << "Pipe " << id_ << " is requesting connection (as replacement)";
  uint64_t token = listener_->registerConnectionRequest(
      callbackWrapper_([connId](
                           PipeImpl& impl,
                           std::string transport,
                           std::shared_ptr<transport::Connection> connection) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done requesting connection (as replacement)";
        if (!impl.error_) {
          impl.onAcceptWhileServerWaitingForConnection(
              connId, std::move(transport), std::move(connection));
        }
      }));
  registrationIds_[connId] = token;

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
    const BrochureAnswer& nopBrochureAnswer) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, CLIENT_WAITING_FOR_BROCHURE_ANSWER);

  const std::string& transport = nopBrochureAnswer.transport;
  std::string address = nopBrochureAnswer.address;
  std::shared_ptr<transport::Context> transportContext =
      context_->getTransport(transport);
  TP_DCHECK(transportContext->canCommunicateWithRemote(
      nopBrochureAnswer.transportDomainDescriptor))
      << "The two endpoints disagree on whether transport " << transport
      << " can be used to communicate";

  if (transport != transport_) {
    TP_VLOG(3) << "Pipe " << id_
               << " is opening connection (descriptor, as replacement)";
    std::shared_ptr<transport::Connection> connection =
        transportContext->connect(address);
    connection->setId(id_ + ".d.tr_" + transport);
    const auto& transportRegistrationIter =
        nopBrochureAnswer.transportRegistrationIds.find(
            ConnectionId::DESCRIPTOR);
    TP_DCHECK(
        transportRegistrationIter !=
        nopBrochureAnswer.transportRegistrationIds.end());
    initConnection(*connection, transportRegistrationIter->second);

    transport_ = transport;
    descriptorConnection_ = std::move(connection);
  }

  {
    TP_VLOG(3) << "Pipe " << id_ << " is opening connection (descriptor_reply)";
    std::shared_ptr<transport::Connection> connection =
        transportContext->connect(address);
    connection->setId(id_ + ".r.tr_" + transport);
    const auto& transportRegistrationIter =
        nopBrochureAnswer.transportRegistrationIds.find(
            ConnectionId::DESCRIPTOR_REPLY);
    TP_DCHECK(
        transportRegistrationIter !=
        nopBrochureAnswer.transportRegistrationIds.end());
    initConnection(*connection, transportRegistrationIter->second);

    descriptorReplyConnection_ = std::move(connection);
  }

  // Recompute the channel map based on this side's channels and priorities.
  SelectedChannels selectedChannels = selectChannels(
      context_->getOrderedChannels(),
      nopBrochureAnswer.channelDeviceDescriptors);
  channelForDevicePair_ = std::move(selectedChannels.channelForDevicePair);

  // Verify that the locally and remotely computed channel maps are consistent.
  TP_THROW_ASSERT_IF(
      nopBrochureAnswer.channelForDevicePair.size() !=
      channelForDevicePair_.size())
      << "Inconsistent channel selection";
  for (const auto& iter : channelForDevicePair_) {
    Device localDevice;
    Device remoteDevice;
    std::tie(localDevice, remoteDevice) = iter.first;
    const std::string& channelName = iter.second;

    const auto& answerIter = nopBrochureAnswer.channelForDevicePair.find(
        {remoteDevice, localDevice});

    TP_THROW_ASSERT_IF(
        answerIter == nopBrochureAnswer.channelForDevicePair.end())
        << "Inconsistent channel selection";
    TP_THROW_ASSERT_IF(answerIter->second != channelName)
        << "Inconsistent channel selection";
  }

  for (const auto& channelDeviceDescriptorsIter :
       selectedChannels.descriptorsMap) {
    const std::string& channelName = channelDeviceDescriptorsIter.first;
    std::shared_ptr<channel::Context> channelContext =
        context_->getChannel(channelName);

    const std::vector<uint64_t>& registrationIds =
        nopBrochureAnswer.channelRegistrationIds.at(channelName);
    const size_t numConnectionsNeeded = channelContext->numConnectionsNeeded();
    TP_DCHECK_EQ(numConnectionsNeeded, registrationIds.size());
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
      initConnection(*connection, registrationIds[connId]);
      connections[connId] = std::move(connection);
    }

    std::shared_ptr<channel::Channel> channel = channelContext->createChannel(
        std::move(connections), channel::Endpoint::kConnect);
    channel->setId(id_ + ".ch_" + channelName);
    channels_.emplace(channelName, std::move(channel));
  }

  state_ = ESTABLISHED;
  readOps_.advanceAllOperations();
  writeOps_.advanceAllOperations();
}

void PipeImpl::initConnection(
    transport::Connection& connection,
    uint64_t token) {
  auto nopHolderOut = std::make_shared<NopHolder<Packet>>();
  Packet& nopPacketOut = nopHolderOut->getObject();
  nopPacketOut.Become(nopPacketOut.index_of<RequestedConnection>());
  RequestedConnection& nopRequestedConnection =
      *nopPacketOut.get<RequestedConnection>();
  nopRequestedConnection.registrationId = token;
  TP_VLOG(3) << "Pipe " << id_
             << " is writing nop object (requested connection)";
  connection.write(
      *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done writing nop object (requested connection)";
      }));
}

void PipeImpl::onAcceptWhileServerWaitingForConnection(
    ConnectionId connId,
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  const auto& registrationIdIter = registrationIds_.find(connId);
  TP_DCHECK(registrationIdIter != registrationIds_.end());
  size_t token = registrationIdIter->second;
  listener_->unregisterConnectionRequest(token);
  registrationIds_.erase(registrationIdIter);
  TP_DCHECK_EQ(transport_, receivedTransport);

  switch (connId) {
    case ConnectionId::DESCRIPTOR:
      receivedConnection->setId(id_ + ".d.tr_" + receivedTransport);
      descriptorConnection_ = std::move(receivedConnection);
      break;
    case ConnectionId::DESCRIPTOR_REPLY:
      receivedConnection->setId(id_ + ".r.tr_" + receivedTransport);
      descriptorReplyConnection_ = std::move(receivedConnection);
      break;
    default:
      TP_THROW_ASSERT() << "Unrecognized connection identifier";
  }

  if (!pendingRegistrations()) {
    state_ = ESTABLISHED;
    readOps_.advanceAllOperations();
    writeOps_.advanceAllOperations();
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
    readOps_.advanceAllOperations();
    writeOps_.advanceAllOperations();
  }
}

bool PipeImpl::pendingRegistrations() {
  if (!registrationIds_.empty()) {
    return true;
  }

  if (!channelRegistrationIds_.empty()) {
    return true;
  }

  return false;
}

} // namespace tensorpipe
