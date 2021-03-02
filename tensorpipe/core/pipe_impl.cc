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
#include <tensorpipe/core/buffer_helpers.h>
#include <tensorpipe/core/context_impl.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/listener_impl.h>
#include <tensorpipe/transport/connection.h>

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
    // FIXME If the nop object wasn't const we could move the string out...
    tensorBeingAllocated.descriptor = nopTensorDescriptor.channelDescriptor;

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
    switch (tensor.buffer.type) {
      case DeviceType::kCpu:
        TP_THROW_ASSERT_IF(
            tensor.buffer.cpu.length != tensorBeingAllocated.length);
        break;
#if TENSORPIPE_SUPPORTS_CUDA
      case DeviceType::kCuda:
        TP_THROW_ASSERT_IF(
            tensor.buffer.cuda.length != tensorBeingAllocated.length);
        break;
#endif // TENSORPIPE_SUPPORTS_CUDA
      default:
        TP_THROW_ASSERT() << "Unexpected device type.";
    }
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
    // FIXME In principle we could move here.
    nopTensorDescriptor.channelDescriptor = otherTensor.descriptor;

    nopTensorDescriptor.deviceType = tensor.buffer.type;
    switch (tensor.buffer.type) {
      case DeviceType::kCpu:
        nopTensorDescriptor.sizeInBytes = tensor.buffer.cpu.length;
        break;
#if TENSORPIPE_SUPPORTS_CUDA
      case DeviceType::kCuda:
        nopTensorDescriptor.sizeInBytes = tensor.buffer.cuda.length;
        break;
#endif // TENSORPIPE_SUPPORTS_CUDA
      default:
        TP_THROW_ASSERT() << "Unknown device type.";
    };
  }

  return nopHolderOut;
}

template <typename TBuffer>
std::unordered_map<std::string, ChannelAdvertisement>& getChannelAdvertisement(
    Brochure& nopBrochure);

template <>
std::unordered_map<std::string, ChannelAdvertisement>& getChannelAdvertisement<
    CpuBuffer>(Brochure& nopBrochure) {
  return nopBrochure.cpuChannelAdvertisement;
}

#if TENSORPIPE_SUPPORTS_CUDA
template <>
std::unordered_map<std::string, ChannelAdvertisement>& getChannelAdvertisement<
    CudaBuffer>(Brochure& nopBrochure) {
  return nopBrochure.cudaChannelAdvertisement;
}
#endif // TENSORPIPE_SUPPORTS_CUDA

template <typename TBuffer>
const std::unordered_map<std::string, ChannelAdvertisement>&
getChannelAdvertisement(const Brochure& nopBrochure);

template <>
const std::unordered_map<std::string, ChannelAdvertisement>&
getChannelAdvertisement<CpuBuffer>(const Brochure& nopBrochure) {
  return nopBrochure.cpuChannelAdvertisement;
}

#if TENSORPIPE_SUPPORTS_CUDA
template <>
const std::unordered_map<std::string, ChannelAdvertisement>&
getChannelAdvertisement<CudaBuffer>(const Brochure& nopBrochure) {
  return nopBrochure.cudaChannelAdvertisement;
}
#endif // TENSORPIPE_SUPPORTS_CUDA

template <typename TBuffer>
std::unordered_map<std::string, ChannelSelection>& getChannelSelection(
    BrochureAnswer& nopBrochureAnswer);

template <>
std::unordered_map<std::string, ChannelSelection>& getChannelSelection<
    CpuBuffer>(BrochureAnswer& nopBrochureAnswer) {
  return nopBrochureAnswer.cpuChannelSelection;
}

#if TENSORPIPE_SUPPORTS_CUDA
template <>
std::unordered_map<std::string, ChannelSelection>& getChannelSelection<
    CudaBuffer>(BrochureAnswer& nopBrochureAnswer) {
  return nopBrochureAnswer.cudaChannelSelection;
}
#endif // TENSORPIPE_SUPPORTS_CUDA

template <typename TBuffer>
const std::unordered_map<std::string, ChannelSelection>& getChannelSelection(
    const BrochureAnswer& nopBrochureAnswer);

template <>
const std::unordered_map<std::string, ChannelSelection>& getChannelSelection<
    CpuBuffer>(const BrochureAnswer& nopBrochureAnswer) {
  return nopBrochureAnswer.cpuChannelSelection;
}

#if TENSORPIPE_SUPPORTS_CUDA
template <>
const std::unordered_map<std::string, ChannelSelection>& getChannelSelection<
    CudaBuffer>(const BrochureAnswer& nopBrochureAnswer) {
  return nopBrochureAnswer.cudaChannelSelection;
}
#endif // TENSORPIPE_SUPPORTS_CUDA

template <typename TBuffer>
TBuffer unwrap(Buffer);

template <>
CpuBuffer unwrap(Buffer b) {
  TP_DCHECK(DeviceType::kCpu == b.type);
  return b.cpu;
}

#if TENSORPIPE_SUPPORTS_CUDA
template <>
CudaBuffer unwrap(Buffer b) {
  TP_DCHECK(DeviceType::kCuda == b.type);
  return b.cuda;
}
#endif // TENSORPIPE_SUPPORTS_CUDA

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

template <>
const std::map<
    int64_t,
    std::tuple<std::string, std::shared_ptr<channel::Context<CpuBuffer>>>>&
PipeImpl::getOrderedChannels() {
  return context_->getOrderedCpuChannels();
}

#if TENSORPIPE_SUPPORTS_CUDA
template <>
const std::map<
    int64_t,
    std::tuple<std::string, std::shared_ptr<channel::Context<CudaBuffer>>>>&
PipeImpl::getOrderedChannels() {
  return context_->getOrderedCudaChannels();
}
#endif // TENSORPIPE_SUPPORTS_CUDA

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
    forEachDeviceType([&](auto buffer) {
      for (const auto& channelContextIter :
           this->getOrderedChannels<decltype(buffer)>()) {
        const std::string& channelName = std::get<0>(channelContextIter.second);
        const channel::Context<decltype(buffer)>& channelContext =
            *(std::get<1>(channelContextIter.second));
        auto& nopChannelAdvertisementMap =
            getChannelAdvertisement<decltype(buffer)>(nopBrochure);
        ChannelAdvertisement& nopChannelAdvertisement =
            nopChannelAdvertisementMap[channelName];
        nopChannelAdvertisement.domainDescriptor =
            channelContext.domainDescriptor();
      }
    });
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

  TP_DCHECK_EQ(op.state, ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE);
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
  TP_DCHECK_EQ(state_, ESTABLISHED);

  ReadOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE);
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
    switchOnDeviceType(
        op.message.tensors[tensorIdx].buffer.type, [&](auto buffer) {
          ReadOperation::Tensor& tensorBeingAllocated = op.tensors[tensorIdx];
          std::shared_ptr<channel::Channel<decltype(buffer)>> channel =
              channels_.get<decltype(buffer)>().at(
                  tensorBeingAllocated.channelName);
          TP_VLOG(3) << "Pipe " << id_ << " is receiving tensor #"
                     << op.sequenceNumber << "." << tensorIdx;

          channel->recv(
              std::move(tensorBeingAllocated.descriptor),
              unwrap<decltype(buffer)>(tensor.buffer),
              callbackWrapper_([opIter, tensorIdx](PipeImpl& impl) {
                TP_VLOG(3) << "Pipe " << impl.id_ << " done receiving tensor #"
                           << opIter->sequenceNumber << "." << tensorIdx;
                impl.onRecvOfTensor(opIter);
              }));
          ++op.numTensorsBeingReceived;
        });
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
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  ReadOperation& op = *opIter;

  TP_DCHECK(
      op.state == ReadOperation::UNINITIALIZED ||
      op.state == ReadOperation::READING_DESCRIPTOR);
  op.state = ReadOperation::ASKING_FOR_ALLOCATION;

  op.readDescriptorCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.readDescriptorCallback = nullptr;
}

void PipeImpl::callReadCallback(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  ReadOperation& op = *opIter;

  TP_DCHECK(
      op.state == ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE ||
      op.state == ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS);
  op.state = ReadOperation::FINISHED;

  op.readCallback(error_, std::move(op.message));
  // Reset callback to release the resources it was holding.
  op.readCallback = nullptr;
}

void PipeImpl::callWriteCallback(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  WriteOperation& op = *opIter;

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
  forEachDeviceType([&](auto buffer) {
    for (auto& channelIter : channels_.get<decltype(buffer)>()) {
      channelIter.second->close();
    }
  });

  if (registrationId_.has_value()) {
    listener_->unregisterConnectionRequest(registrationId_.value());
    registrationId_.reset();
  }
  forEachDeviceType([&](auto buffer) {
    for (const auto& iter : channelRegistrationIds_.get<decltype(buffer)>()) {
      for (const auto& token : iter.second) {
        listener_->unregisterConnectionRequest(token);
      }
    }
    channelRegistrationIds_.get<decltype(buffer)>().clear();
    channelReceivedConnections_.get<decltype(buffer)>().clear();
  });

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
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::UNINITIALIZED,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*cond=*/error_ && prevOpState >= ReadOperation::ASKING_FOR_ALLOCATION,
      /*action=*/&PipeImpl::callReadDescriptorCallback);

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
      /*action=*/&PipeImpl::readDescriptorOfMessage);

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::READING_DESCRIPTOR,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*cond=*/(error_ || opIter->doneReadingDescriptor) &&
          prevOpState >= ReadOperation::ASKING_FOR_ALLOCATION,
      /*action=*/&PipeImpl::callReadDescriptorCallback);

  // Needs to wait for previous op to have _received_ the read call, as we can
  // only have exactly one operation at a time for which we expect a read call.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION,
      /*to=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*cond=*/(error_ || opIter->doneReadingDescriptor) &&
          prevOpState >= ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*action=*/&PipeImpl::expectReadCall);

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/error_ && opIter->doneGettingAllocation &&
          prevOpState >= ReadOperation::FINISHED,
      /*action=*/&PipeImpl::callReadCallback);

  // No need to order this with the previous operation, since all it needs is
  // to come after this own op's descriptor read.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE,
      /*to=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*cond=*/!error_ && opIter->doneGettingAllocation,
      /*action=*/&PipeImpl::readPayloadsAndReceiveTensorsOfMessage);

  // Needs to go after previous op to ensure ordering of callback invocations.
  readOps_.attemptTransition(
      opIter,
      /*from=*/ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS,
      /*to=*/ReadOperation::FINISHED,
      /*cond=*/opIter->numPayloadsBeingRead == 0 &&
          opIter->numTensorsBeingReceived == 0 &&
          prevOpState >= ReadOperation::FINISHED,
      /*action=*/&PipeImpl::callReadCallback);
}

void PipeImpl::advanceWriteOperation(
    WriteOpIter opIter,
    WriteOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_ && prevOpState >= WriteOperation::FINISHED,
      /*action=*/&PipeImpl::callWriteCallback);

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of send calls on channels.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::UNINITIALIZED,
      /*to=*/WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >=
              WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
      /*action=*/&PipeImpl::sendTensorsOfMessage);

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/error_ && opIter->numTensorDescriptorsBeingCollected == 0 &&
          opIter->numTensorsBeingSent == 0 &&
          prevOpState >= WriteOperation::FINISHED,
      /*action=*/&PipeImpl::callWriteCallback);

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the connection.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS,
      /*to=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*cond=*/!error_ && opIter->numTensorDescriptorsBeingCollected == 0 &&
          prevOpState >= WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*action=*/&PipeImpl::writeDescriptorAndPayloadsOfMessage);

  // Needs to go after previous op to ensure ordering of callback invocations.
  writeOps_.attemptTransition(
      opIter,
      /*from=*/WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS,
      /*to=*/WriteOperation::FINISHED,
      /*cond=*/opIter->numPayloadsBeingWritten == 0 &&
          opIter->numTensorsBeingSent == 0 &&
          prevOpState >= WriteOperation::FINISHED,
      /*action=*/&PipeImpl::callWriteCallback);
}

void PipeImpl::readDescriptorOfMessage(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  ReadOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, ReadOperation::UNINITIALIZED);
  op.state = ReadOperation::READING_DESCRIPTOR;

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
        if (!impl.error_) {
          impl.onReadOfMessageDescriptor(opIter, nopHolderIn->getObject());
        }
      }));
  connectionState_ = AWAITING_PAYLOADS;
}

void PipeImpl::expectReadCall(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  ReadOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, ReadOperation::ASKING_FOR_ALLOCATION);
  op.state = ReadOperation::ASKING_FOR_ALLOCATION_FIRST_IN_LINE;

  TP_DCHECK(!nextMessageGettingAllocation_.has_value());
  nextMessageGettingAllocation_ = opIter;
}

void PipeImpl::sendTensorsOfMessage(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  WriteOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, WriteOperation::UNINITIALIZED);
  op.state = WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS;

  TP_VLOG(2) << "Pipe " << id_ << " is sending tensors of message #"
             << op.sequenceNumber;

  for (int tensorIdx = 0; tensorIdx < op.message.tensors.size(); ++tensorIdx) {
    const auto& tensor = op.message.tensors[tensorIdx];

    auto t = switchOnDeviceType(tensor.buffer.type, [&](auto buffer) {
      auto& orderedChannels = this->getOrderedChannels<decltype(buffer)>();
      auto& availableChannels = channels_.get<decltype(buffer)>();
      for (const auto& channelContextIter : orderedChannels) {
        const std::string& channelName = std::get<0>(channelContextIter.second);
        auto channelIter = availableChannels.find(channelName);
        if (channelIter == availableChannels.cend()) {
          continue;
        }
        auto& channel = *(channelIter->second);

        TP_VLOG(3) << "Pipe " << id_ << " is sending tensor #"
                   << op.sequenceNumber << "." << tensorIdx;

        channel.send(
            unwrap<decltype(buffer)>(tensor.buffer),
            callbackWrapper_([opIter, tensorIdx](
                                 PipeImpl& impl,
                                 channel::TDescriptor descriptor) {
              TP_VLOG(3) << "Pipe " << impl.id_ << " got tensor descriptor #"
                         << opIter->sequenceNumber << "." << tensorIdx;
              impl.onDescriptorOfTensor(
                  opIter, tensorIdx, std::move(descriptor));
            }),
            callbackWrapper_([opIter, tensorIdx](PipeImpl& impl) {
              TP_VLOG(3) << "Pipe " << impl.id_ << " done sending tensor #"
                         << opIter->sequenceNumber << "." << tensorIdx;
              impl.onSendOfTensor(opIter);
            }));
        return WriteOperation::Tensor{tensor.buffer.type, channelName};
      }

      TP_THROW_ASSERT() << "Could not find channel.";
      return WriteOperation::Tensor{};
    });
    op.tensors.push_back(t);

    ++op.numTensorDescriptorsBeingCollected;
    ++op.numTensorsBeingSent;
  }
}

void PipeImpl::writeDescriptorAndPayloadsOfMessage(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  WriteOperation& op = *opIter;

  TP_DCHECK_EQ(
      op.state, WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS);
  op.state = WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS;

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

    const auto nopTransportAdvertisementIter =
        nopBrochure.transportAdvertisement.find(transportName);
    if (nopTransportAdvertisementIter ==
        nopBrochure.transportAdvertisement.cend()) {
      continue;
    }
    const TransportAdvertisement& nopTransportAdvertisement =
        nopTransportAdvertisementIter->second;
    const std::string& domainDescriptor =
        nopTransportAdvertisement.domainDescriptor;
    if (!transportContext.canCommunicateWithRemote(domainDescriptor)) {
      continue;
    }

    nopBrochureAnswer.transport = transportName;
    nopBrochureAnswer.address = address;
    nopBrochureAnswer.transportDomainDescriptor =
        transportContext.domainDescriptor();

    if (transportName != transport_) {
      transport_ = transportName;
      TP_DCHECK(!registrationId_.has_value());
      TP_VLOG(3) << "Pipe " << id_
                 << " is requesting connection (as replacement)";
      uint64_t token = listener_->registerConnectionRequest(callbackWrapper_(
          [](PipeImpl& impl,
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
      needToWaitForConnections = true;
      nopBrochureAnswer.transportRegistrationId = token;
    }

    foundATransport = true;
    break;
  }
  TP_THROW_ASSERT_IF(!foundATransport);

  forEachDeviceType([&](auto buffer) {
    for (const auto& channelContextIter :
         this->getOrderedChannels<decltype(buffer)>()) {
      const std::string& channelName = std::get<0>(channelContextIter.second);
      const channel::Context<decltype(buffer)>& channelContext =
          *(std::get<1>(channelContextIter.second));

      const auto& nopChannelAdvertisementMap =
          getChannelAdvertisement<decltype(buffer)>(nopBrochure);
      const auto nopChannelAdvertisementIter =
          nopChannelAdvertisementMap.find(channelName);
      if (nopChannelAdvertisementIter == nopChannelAdvertisementMap.cend()) {
        continue;
      }
      const ChannelAdvertisement& nopChannelAdvertisement =
          nopChannelAdvertisementIter->second;
      const std::string& domainDescriptor =
          nopChannelAdvertisement.domainDescriptor;
      if (!channelContext.canCommunicateWithRemote(domainDescriptor)) {
        continue;
      }

      const size_t numConnectionsNeeded = channelContext.numConnectionsNeeded();
      auto& channelRegistrationIds =
          channelRegistrationIds_.get<decltype(buffer)>()[channelName];
      channelRegistrationIds.resize(numConnectionsNeeded);
      auto& channelReceivedConnections =
          channelReceivedConnections_.get<decltype(buffer)>()[channelName];
      channelReceivedConnections.resize(numConnectionsNeeded);
      for (size_t connId = 0; connId < numConnectionsNeeded; ++connId) {
        TP_VLOG(3) << "Pipe " << id_ << " is requesting connection " << connId
                   << "/" << numConnectionsNeeded << " (for channel "
                   << channelName << ")";
        uint64_t token = listener_->registerConnectionRequest(callbackWrapper_(
            [channelName, connId, numConnectionsNeeded](
                PipeImpl& impl,
                std::string transport,
                std::shared_ptr<transport::Connection> connection) {
              TP_VLOG(3) << "Pipe " << impl.id_
                         << " done requesting connection " << connId << "/"
                         << numConnectionsNeeded << " (for channel "
                         << channelName << ")";
              if (!impl.error_) {
                impl.onAcceptWhileServerWaitingForChannel<decltype(buffer)>(
                    channelName,
                    connId,
                    std::move(transport),
                    std::move(connection));
              }
            }));
        channelRegistrationIds[connId] = token;
        needToWaitForConnections = true;
      }
      auto& nopChannelSelectionMap =
          getChannelSelection<decltype(buffer)>(nopBrochureAnswer);
      ChannelSelection& nopChannelSelection =
          nopChannelSelectionMap[channelName];
      nopChannelSelection.registrationIds = channelRegistrationIds;
      nopChannelSelection.domainDescriptor = channelContext.domainDescriptor();
    }
  });

  TP_VLOG(3) << "Pipe " << id_ << " is writing nop object (brochure answer)";
  connection_->write(
      *nopHolderOut, callbackWrapper_([nopHolderOut](PipeImpl& impl) {
        TP_VLOG(3) << "Pipe " << impl.id_
                   << " done writing nop object (brochure answer)";
      }));

  if (!needToWaitForConnections) {
    state_ = ESTABLISHED;
    startReadingUponEstablishingPipe();
    startWritingUponEstablishingPipe();
  } else {
    state_ = SERVER_WAITING_FOR_CONNECTIONS;
  }
}

template <>
std::shared_ptr<channel::Context<CpuBuffer>> PipeImpl::getChannelContext(
    const std::string& channelName) {
  return context_->getCpuChannel(channelName);
}

#if TENSORPIPE_SUPPORTS_CUDA
template <>
std::shared_ptr<channel::Context<CudaBuffer>> PipeImpl::getChannelContext(
    const std::string& channelName) {
  return context_->getCudaChannel(channelName);
}
#endif // TENSORPIPE_SUPPORTS_CUDA

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

  forEachDeviceType([&](auto buffer) {
    for (const auto& nopChannelSelectionIter :
         getChannelSelection<decltype(buffer)>(nopBrochureAnswer)) {
      const std::string& channelName = nopChannelSelectionIter.first;
      const ChannelSelection& nopChannelSelection =
          nopChannelSelectionIter.second;

      std::shared_ptr<channel::Context<decltype(buffer)>> channelContext =
          this->getChannelContext<decltype(buffer)>(channelName);
      TP_DCHECK(channelContext->canCommunicateWithRemote(
          nopChannelSelection.domainDescriptor))
          << "The two endpoints disagree on whether channel " << channelName
          << " can be used to communicate";

      const size_t numConnectionsNeeded =
          channelContext->numConnectionsNeeded();
      TP_DCHECK_EQ(
          numConnectionsNeeded, nopChannelSelection.registrationIds.size());
      std::vector<std::shared_ptr<transport::Connection>> connections(
          numConnectionsNeeded);
      for (size_t connId = 0; connId < numConnectionsNeeded; ++connId) {
        TP_VLOG(3) << "Pipe " << id_ << " is opening connection " << connId
                   << "/" << numConnectionsNeeded << " (for channel "
                   << channelName << ")";
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

      std::shared_ptr<channel::Channel<decltype(buffer)>> channel =
          channelContext->createChannel(
              std::move(connections), channel::Endpoint::kConnect);
      channel->setId(id_ + ".ch_" + channelName);
      channels_.get<decltype(buffer)>().emplace(
          channelName, std::move(channel));
    }
  });

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

template <typename TBuffer>
void PipeImpl::onAcceptWhileServerWaitingForChannel(
    std::string channelName,
    size_t connId,
    std::string receivedTransport,
    std::shared_ptr<transport::Connection> receivedConnection) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, SERVER_WAITING_FOR_CONNECTIONS);
  TP_DCHECK_EQ(transport_, receivedTransport);
  auto& channelRegistrationIds = channelRegistrationIds_.get<TBuffer>();
  auto channelRegistrationIdsIter = channelRegistrationIds.find(channelName);
  TP_DCHECK(channelRegistrationIdsIter != channelRegistrationIds.end());
  listener_->unregisterConnectionRequest(
      channelRegistrationIdsIter->second[connId]);
  receivedConnection->setId(
      id_ + ".ch_" + channelName + "_" + std::to_string(connId));

  auto& channelReceivedConnections = channelReceivedConnections_.get<TBuffer>();

  channelReceivedConnections[channelName][connId] =
      std::move(receivedConnection);
  // TODO: If we can guarantee the order in which the accept() calls happen,
  // this check can be replaced with `if (connId == numConnectionsNeeded -
  // 1)`.
  for (const auto& conn : channelReceivedConnections[channelName]) {
    if (conn == nullptr) {
      return;
    }
  }

  std::shared_ptr<channel::Context<TBuffer>> channelContext =
      getChannelContext<TBuffer>(channelName);

  std::shared_ptr<channel::Channel<TBuffer>> channel =
      channelContext->createChannel(
          std::move(channelReceivedConnections[channelName]),
          channel::Endpoint::kListen);
  channel->setId(id_ + ".ch_" + channelName);

  channelRegistrationIds.erase(channelRegistrationIdsIter);
  channelReceivedConnections.erase(channelName);

  auto& channels = channels_.get<TBuffer>();
  TP_DCHECK(channels.find(channelName) == channels.end());
  channels.emplace(channelName, std::move(channel));

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
  TP_DCHECK_EQ(state_, ESTABLISHED);

  ReadOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, ReadOperation::READING_DESCRIPTOR);
  parseDescriptorOfMessage(op, nopPacketIn);
  op.doneReadingDescriptor = true;

  readOps_.advanceOperation(opIter);
}

void PipeImpl::onDescriptorOfTensor(
    WriteOpIter opIter,
    int64_t tensorIdx,
    channel::TDescriptor descriptor) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  TP_DCHECK_EQ(
      op.state, WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS);
  TP_DCHECK_LT(tensorIdx, op.tensors.size());
  op.tensors[tensorIdx].descriptor = std::move(descriptor);
  --op.numTensorDescriptorsBeingCollected;

  writeOps_.advanceOperation(opIter);
}

void PipeImpl::onReadOfPayload(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS);
  op.numPayloadsBeingRead--;

  readOps_.advanceOperation(opIter);
}

void PipeImpl::onRecvOfTensor(ReadOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  ReadOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, ReadOperation::READING_PAYLOADS_AND_RECEIVING_TENSORS);
  op.numTensorsBeingReceived--;

  readOps_.advanceOperation(opIter);
}

void PipeImpl::onWriteOfPayload(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  TP_DCHECK_EQ(op.state, WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS);
  op.numPayloadsBeingWritten--;

  writeOps_.advanceOperation(opIter);
}

void PipeImpl::onSendOfTensor(WriteOpIter opIter) {
  TP_DCHECK(context_->inLoop());

  WriteOperation& op = *opIter;

  TP_DCHECK_GE(
      op.state, WriteOperation::SENDING_TENSORS_AND_COLLECTING_DESCRIPTORS);
  TP_DCHECK_LE(op.state, WriteOperation::WRITING_PAYLOADS_AND_SENDING_TENSORS);
  op.numTensorsBeingSent--;

  writeOps_.advanceOperation(opIter);
}

bool PipeImpl::pendingRegistrations() {
  if (registrationId_.has_value()) {
    return true;
  }

  bool ret = false;
  forEachDeviceType([&](auto buffer) {
    if (!channelRegistrationIds_.get<decltype(buffer)>().empty()) {
      ret = true;
    }
  });

  return ret;
}

} // namespace tensorpipe
