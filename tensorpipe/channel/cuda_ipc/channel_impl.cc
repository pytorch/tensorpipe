/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_ipc/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <cuda.h>
#include <cuda_runtime.h>
#include <nop/serializer.h>
#include <nop/structure.h>
#include <nop/types/variant.h>

#include <tensorpipe/channel/cuda_ipc/constants.h>
#include <tensorpipe/channel/cuda_ipc/context_impl.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

NOP_EXTERNAL_STRUCTURE(
    ContextImpl::OutboxInfo,
    processIdentifier,
    memHandle,
    eventHandles);

namespace {

size_t ceilOfRatio(size_t n, size_t d) {
  return (n + d - 1) / d;
}

struct Descriptor {
  int deviceIdx;
  size_t slotIdx;
  nop::Optional<ContextImpl::OutboxInfo> outboxInfo;
  NOP_STRUCTURE(Descriptor, deviceIdx, slotIdx, outboxInfo);
};

} // namespace

ChunkSendOperation::ChunkSendOperation(
    uint64_t bufferSequenceNumber,
    size_t chunkId,
    size_t numChunks,
    TSendCallback callback,
    int deviceIdx,
    const void* ptr,
    size_t length,
    cudaStream_t stream)
    : bufferSequenceNumber(bufferSequenceNumber),
      chunkId(chunkId),
      numChunks(numChunks),
      ptr(ptr),
      length(length),
      deviceIdx(deviceIdx),
      stream(stream),
      callback(std::move(callback)) {}

ChunkRecvOperation::ChunkRecvOperation(
    uint64_t bufferSequenceNumber,
    size_t chunkId,
    size_t numChunks,
    TRecvCallback callback,
    int deviceIdx,
    void* ptr,
    size_t length,
    cudaStream_t stream)
    : bufferSequenceNumber(bufferSequenceNumber),
      chunkId(chunkId),
      numChunks(numChunks),
      ptr(ptr),
      length(length),
      deviceIdx(deviceIdx),
      stream(stream),
      callback(std::move(callback)) {}

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> descriptorConnection,
    std::shared_ptr<transport::Connection> replyConnection)
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      descriptorConnection_(std::move(descriptorConnection)),
      replyConnection_(std::move(replyConnection)) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    size_t length,
    TSendCallback callback) {
  if (length == 0) {
    callback(error_);
    return;
  }

  int deviceIdx = cudaDeviceForPointer(
      context_->getCudaLib(), buffer.unwrap<CudaBuffer>().ptr);
  const size_t numChunks = ceilOfRatio(length, kSlotSize);

  for (size_t chunkIdx = 0; chunkIdx < numChunks; chunkIdx += 1) {
    size_t offset = chunkIdx * kSlotSize;
    ChunkSendOpIter opIter = chunkSendOps_.emplaceBack(
        nextChunkBeingSent_++,
        sequenceNumber,
        chunkIdx,
        numChunks,
        chunkIdx == numChunks - 1 ? std::move(callback) : nullptr,
        deviceIdx,
        reinterpret_cast<uint8_t*>(buffer.unwrap<CudaBuffer>().ptr) + offset,
        std::min(length - offset, kSlotSize),
        buffer.unwrap<CudaBuffer>().stream);

    chunkSendOps_.advanceOperation(opIter);
  }
}

void ChannelImpl::advanceChunkSendOperation(
    ChunkSendOpIter opIter,
    ChunkSendOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  ChunkSendOperation& op = *opIter;

  // Needs to go after previous op invoked its callback because the last chunk
  // in a series (that corresponds to one operation) must invoke its callback
  // only when all chunks in the series are done.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::UNINITIALIZED,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_ && prevOpState >= ChunkSendOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure later operations are not holding
  // events while earlier ones are still blocked waiting for them, because the
  // events will only be returned after the control messages have been written
  // and sent, and this won't happen for later operations until earlier ones
  // have reached that stage too, and if those are blocked waiting for events
  // then we may deadlock.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::UNINITIALIZED,
      /*to=*/ChunkSendOperation::ALLOCATING_STAGING_BUFFER,
      /*cond=*/!error_ &&
          prevOpState >= ChunkSendOperation::ALLOCATING_STAGING_BUFFER,
      /*actions=*/
      {&ChannelImpl::allocateStagingBuffer});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/
      ChunkSendOperation::ALLOCATING_STAGING_BUFFER,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_ && op.doneAllocatingStagingBuffer &&
          prevOpState >= ChunkSendOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::callSendCallback, &ChannelImpl::releaseStagingBuffer});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the descriptor control connection and read calls on the
  // reply control connection.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/
      ChunkSendOperation::ALLOCATING_STAGING_BUFFER,
      /*to=*/ChunkSendOperation::READING_REPLY,
      /*cond=*/!error_ && op.doneAllocatingStagingBuffer &&
          prevOpState >= ChunkSendOperation::READING_REPLY,
      /*actions=*/
      {&ChannelImpl::copyFromSourceToStaging,
       &ChannelImpl::writeDescriptor,
       &ChannelImpl::readReply,
       &ChannelImpl::callSendCallback});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::READING_REPLY,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/op.doneReadingReply &&
          prevOpState >= ChunkSendOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::releaseStagingBuffer});
}

void ChannelImpl::allocateStagingBuffer(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  TP_VLOG(5) << "Channel " << id_
             << " is allocating temporary memory for chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber;
  context_->allocateSlot(
      op.deviceIdx,
      op.length,
      callbackWrapper_([opIter](
                           ChannelImpl& impl,
                           size_t slotIdx,
                           Allocator::TChunk buffer,
                           CudaEvent* event) {
        TP_VLOG(5) << "Channel " << impl.id_
                   << " is done allocating temporary memory for chunk #"
                   << opIter->chunkId << " of " << opIter->numChunks
                   << " for buffer #" << opIter->bufferSequenceNumber;
        opIter->doneAllocatingStagingBuffer = true;
        if (!impl.error_) {
          opIter->slotIdx = slotIdx;
          opIter->stagingBuffer = std::move(buffer);
          opIter->event = event;
        }
        impl.chunkSendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::copyFromSourceToStaging(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  op.event->wait(op.stream, op.deviceIdx);
  {
    CudaDeviceGuard guard(op.deviceIdx);
    TP_CUDA_CHECK(cudaMemcpyAsync(
        op.stagingBuffer.get(),
        op.ptr,
        op.length,
        cudaMemcpyDeviceToDevice,
        op.stream));
  }
  op.event->record(op.stream);
}

void ChannelImpl::writeDescriptor(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  const CudaLib& cudaLib = context_->getCudaLib();

  auto nopDescriptorHolder = std::make_shared<NopHolder<Descriptor>>();
  Descriptor& nopDescriptor = nopDescriptorHolder->getObject();
  nopDescriptor.deviceIdx = op.deviceIdx;
  nopDescriptor.slotIdx = op.slotIdx;
  if (localOutboxesSent_.size() <= op.deviceIdx) {
    localOutboxesSent_.resize(op.deviceIdx + 1, false);
  }
  if (!localOutboxesSent_[op.deviceIdx]) {
    localOutboxesSent_[op.deviceIdx] = true;
    nopDescriptor.outboxInfo = context_->getLocalOutboxInfo(op.deviceIdx);
  }

  TP_VLOG(6) << "Channel " << id_ << " is writing nop object (descriptor #"
             << op.sequenceNumber << ")";
  descriptorConnection_->write(
      *nopDescriptorHolder,
      callbackWrapper_([nopDescriptorHolder,
                        sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (descriptor #" << sequenceNumber
                   << ")";
      }));
}

void ChannelImpl::readReply(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (reply #"
             << op.sequenceNumber << ")";
  replyConnection_->read(
      nullptr,
      0,
      callbackWrapper_([opIter](
                           ChannelImpl& impl,
                           const void* /* unused */,
                           size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (reply #"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingReply = true;
        impl.chunkSendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::releaseStagingBuffer(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  op.stagingBuffer = nullptr;
}

void ChannelImpl::callSendCallback(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  if (op.callback) {
    op.callback(error_);
    // Reset callback to release the resources it was holding.
    op.callback = nullptr;
  }
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    Buffer buffer,
    size_t length,
    TRecvCallback callback) {
  if (length == 0) {
    callback(error_);
    return;
  }

  int deviceIdx = cudaDeviceForPointer(
      context_->getCudaLib(), buffer.unwrap<CudaBuffer>().ptr);
  const size_t numChunks = ceilOfRatio(length, kSlotSize);

  for (size_t chunkIdx = 0; chunkIdx < numChunks; chunkIdx += 1) {
    size_t offset = chunkIdx * kSlotSize;
    ChunkRecvOpIter opIter = chunkRecvOps_.emplaceBack(
        nextChunkBeingReceived_++,
        sequenceNumber,
        chunkIdx,
        numChunks,
        chunkIdx == numChunks - 1 ? std::move(callback) : nullptr,
        deviceIdx,
        reinterpret_cast<uint8_t*>(buffer.unwrap<CudaBuffer>().ptr) + offset,
        std::min(length - offset, kSlotSize),
        buffer.unwrap<CudaBuffer>().stream);

    chunkRecvOps_.advanceOperation(opIter);
  }
}

void ChannelImpl::advanceChunkRecvOperation(
    ChunkRecvOpIter opIter,
    ChunkRecvOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());

  ChunkRecvOperation& op = *opIter;

  // Needs to go after previous op invoked its callback because the last chunk
  // in a series (that corresponds to one operation) must invoke its callback
  // only when all chunks in the series are done.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::UNINITIALIZED,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/error_ && prevOpState >= ChunkRecvOperation::FINISHED,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on descriptor control connection.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::UNINITIALIZED,
      /*to=*/ChunkRecvOperation::READING_DESCRIPTOR,
      /*cond=*/!error_ && prevOpState >= ChunkRecvOperation::READING_DESCRIPTOR,
      /*actions=*/{&ChannelImpl::readDescriptor});

  // See above for why this needs to go after previous op.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::READING_DESCRIPTOR,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/error_ && op.doneReadingDescriptor &&
          prevOpState >= ChunkRecvOperation::FINISHED,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on reply control connection.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::READING_DESCRIPTOR,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/!error_ && op.doneReadingDescriptor &&
          prevOpState >= ChunkRecvOperation::FINISHED,
      /*actions=*/
      {&ChannelImpl::copyFromStagingToTarget,
       &ChannelImpl::writeReply,
       &ChannelImpl::callRecvCallback});
}

void ChannelImpl::readDescriptor(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (descriptor #"
             << op.sequenceNumber << ")";
  auto nopDescriptorHolder = std::make_shared<NopHolder<Descriptor>>();
  descriptorConnection_->read(
      *nopDescriptorHolder,
      callbackWrapper_([opIter, nopDescriptorHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (descriptor #"
                   << opIter->sequenceNumber << ")";
        opIter->doneReadingDescriptor = true;
        if (!impl.error_) {
          Descriptor& nopDescriptor = nopDescriptorHolder->getObject();
          opIter->remoteDeviceIdx = nopDescriptor.deviceIdx;
          opIter->remoteSlotIdx = nopDescriptor.slotIdx;
          if (!nopDescriptor.outboxInfo.empty()) {
            if (impl.remoteOutboxesReceived_.size() <=
                opIter->remoteDeviceIdx) {
              impl.remoteOutboxesReceived_.resize(opIter->remoteDeviceIdx + 1);
            }
            TP_DCHECK(!impl.remoteOutboxesReceived_[opIter->remoteDeviceIdx]
                           .has_value());
            impl.remoteOutboxesReceived_[opIter->remoteDeviceIdx] =
                std::move(nopDescriptor.outboxInfo.take());
          }
        }
        impl.chunkRecvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::copyFromStagingToTarget(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  if (remoteOutboxesOpened_.size() <= op.remoteDeviceIdx) {
    remoteOutboxesOpened_.resize(op.remoteDeviceIdx + 1);
  }
  if (remoteOutboxesOpened_[op.remoteDeviceIdx].size() <= op.deviceIdx) {
    remoteOutboxesOpened_[op.remoteDeviceIdx].resize(op.deviceIdx + 1, nullptr);
  }
  if (remoteOutboxesOpened_[op.remoteDeviceIdx][op.deviceIdx] == nullptr) {
    remoteOutboxesOpened_[op.remoteDeviceIdx][op.deviceIdx] =
        &context_->openRemoteOutbox(
            op.deviceIdx,
            op.remoteDeviceIdx,
            remoteOutboxesReceived_[op.remoteDeviceIdx].value());
  }
  const ContextImpl::RemoteOutboxHandle& outbox =
      *remoteOutboxesOpened_[op.remoteDeviceIdx][op.deviceIdx];

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#"
             << op.sequenceNumber << ")";

  outbox.events[op.remoteSlotIdx]->wait(op.stream, op.deviceIdx);
  {
    CudaDeviceGuard guard(op.deviceIdx);
    TP_CUDA_CHECK(cudaMemcpyAsync(
        op.ptr,
        outbox.buffer.ptr() + kSlotSize * op.remoteSlotIdx,
        op.length,
        cudaMemcpyDeviceToDevice,
        op.stream));
  }
  outbox.events[op.remoteSlotIdx]->record(op.stream);

  TP_VLOG(6) << "Channel " << id_ << " done copying payload (#"
             << op.sequenceNumber << ")";
}

void ChannelImpl::callRecvCallback(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  if (op.callback) {
    op.callback(error_);
    // Reset callback to release the resources it was holding.
    op.callback = nullptr;
  }
}

void ChannelImpl::writeReply(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is writing reply notification (#"
             << op.sequenceNumber << ")";
  replyConnection_->write(
      nullptr,
      0,
      callbackWrapper_([sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing reply notification (#" << sequenceNumber
                   << ")";
      }));
}

void ChannelImpl::handleErrorImpl() {
  chunkSendOps_.advanceAllOperations();
  chunkRecvOps_.advanceAllOperations();

  descriptorConnection_->close();
  replyConnection_->close();

  context_->unenroll(*this);
}

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
