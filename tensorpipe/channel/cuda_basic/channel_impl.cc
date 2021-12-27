/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_basic/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <cuda_runtime.h>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/channel/cuda_basic/constants.h>
#include <tensorpipe/channel/cuda_basic/context_impl.h>
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

namespace {

size_t ceilOfRatio(size_t n, size_t d) {
  return (n + d - 1) / d;
}

} // namespace

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection,
    std::shared_ptr<Channel> cpuChannel,
    CudaLoop& cudaLoop)
    : ChannelImplBoilerplate<ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)),
      cpuChannel_(std::move(cpuChannel)),
      cudaLoop_(cudaLoop) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::cudaCopy(
    void* dst,
    const void* src,
    size_t length,
    int deviceIdx,
    cudaStream_t stream,
    std::function<void(const Error&)> callback) {
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault, stream));
  }

  cudaLoop_.addCallback(deviceIdx, stream, std::move(callback));
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

  const Device device = buffer.device();
  const size_t chunkLength = kSlotSize;
  const size_t numChunks = ceilOfRatio(length, chunkLength);
  for (size_t offset = 0; offset < length; offset += chunkLength) {
    ChunkSendOpIter opIter = chunkSendOps_.emplaceBack(nextChunkBeingSent_++);
    ChunkSendOperation& op = *opIter;
    op.bufferSequenceNumber = sequenceNumber;
    op.chunkId = offset / chunkLength;
    op.numChunks = numChunks;
    op.length = std::min(length - offset, chunkLength);
    // Operations are processed in order, so we can afford to trigger the
    // callback once the last operation is done.
    if (op.chunkId == numChunks - 1) {
      op.callback = std::move(callback);
    }

    if (device.type == kCpuDeviceType) {
      op.isCpuBuffer = true;
      op.devicePtr =
          static_cast<uint8_t*>(buffer.unwrap<CpuBuffer>().ptr) + offset;
    } else if (device.type == kCudaDeviceType) {
      op.isCpuBuffer = false;
      op.devicePtr =
          static_cast<uint8_t*>(buffer.unwrap<CudaBuffer>().ptr) + offset;
      op.stream = buffer.unwrap<CudaBuffer>().stream;
      op.deviceIdx = device.index;
    } else {
      TP_THROW_ASSERT() << "Unexpected device type: " << device.type;
    }

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
      /*cond=*/error_ && prevOpState >= ChunkSendOperation::INVOKED_CALLBACK,
      /*actions=*/{&ChannelImpl::callSendCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of send calls on CPU channel.
  // This transition shortcuts the allocation of/copy to staging memory when the
  // buffer is already on CPU.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::UNINITIALIZED,
      /*to=*/ChunkSendOperation::SENDING_CPU_BUFFER,
      /*cond=*/!error_ && op.isCpuBuffer &&
          prevOpState >= ChunkSendOperation::SENDING_CPU_BUFFER,
      /*actions=*/
      {&ChannelImpl::writeReadyToSend, &ChannelImpl::sendCpuBuffer});

  // Needs to go after previous op to ensure later operations are not holding
  // staging buffers while earlier ones are still blocked waiting for them,
  // because the staging buffer will only be returned to the allocator once the
  // operation is destroyed, but this won't happen until earlier operations have
  // completed, and if they are blocked waiting for buffers we may deadlock.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::UNINITIALIZED,
      /*to=*/ChunkSendOperation::ALLOCATING_CPU_BUFFER,
      /*cond=*/!error_ && !op.isCpuBuffer &&
          prevOpState >= ChunkSendOperation::ALLOCATING_CPU_BUFFER,
      /*actions=*/{&ChannelImpl::allocateSendCpuBuffer});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::ALLOCATING_CPU_BUFFER,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_ && op.doneAllocatingCpuStagingBuffer &&
          prevOpState >= ChunkSendOperation::INVOKED_CALLBACK,
      /*actions=*/
      {&ChannelImpl::callSendCallback, &ChannelImpl::returnSendCpuBuffer});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of write calls on the control connection.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::ALLOCATING_CPU_BUFFER,
      /*to=*/ChunkSendOperation::COPYING_FROM_GPU_TO_CPU,
      /*cond=*/!error_ && op.doneAllocatingCpuStagingBuffer &&
          prevOpState >= ChunkSendOperation::COPYING_FROM_GPU_TO_CPU,
      /*actions=*/
      {&ChannelImpl::writeReadyToSend, &ChannelImpl::copyFromGpuToCpu});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::COPYING_FROM_GPU_TO_CPU,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_ && op.doneCopyingFromGpuToCpu &&
          prevOpState >= ChunkSendOperation::INVOKED_CALLBACK,
      /*actions=*/
      {&ChannelImpl::callSendCallback, &ChannelImpl::returnSendCpuBuffer});

  // See above for why this needs to go after previous op.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::COPYING_FROM_GPU_TO_CPU,
      /*to=*/ChunkSendOperation::INVOKED_CALLBACK,
      /*cond=*/!error_ && op.doneCopyingFromGpuToCpu &&
          prevOpState >= ChunkSendOperation::INVOKED_CALLBACK,
      /*actions=*/{&ChannelImpl::callSendCallback});

  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::INVOKED_CALLBACK,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/error_,
      /*actions=*/{&ChannelImpl::returnSendCpuBuffer});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of send calls on CPU channel.
  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::INVOKED_CALLBACK,
      /*to=*/ChunkSendOperation::SENDING_CPU_BUFFER,
      /*cond=*/!error_ && prevOpState >= ChunkSendOperation::SENDING_CPU_BUFFER,
      /*actions=*/{&ChannelImpl::sendCpuBuffer});

  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::SENDING_CPU_BUFFER,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/op.doneSendingCpuBuffer && op.isCpuBuffer,
      /*actions=*/{&ChannelImpl::callSendCallback});

  chunkSendOps_.attemptTransition(
      opIter,
      /*from=*/ChunkSendOperation::SENDING_CPU_BUFFER,
      /*to=*/ChunkSendOperation::FINISHED,
      /*cond=*/op.doneSendingCpuBuffer && !op.isCpuBuffer,
      /*actions=*/{&ChannelImpl::returnSendCpuBuffer});
}

void ChannelImpl::allocateSendCpuBuffer(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  TP_VLOG(5) << "Channel " << id_
             << " is allocating temporary memory for chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber;
  Allocator& cudaHostAllocator =
      context_->getCudaHostSendAllocator(op.deviceIdx);
  cudaHostAllocator.alloc(
      op.length,
      callbackWrapper_(
          [opIter](ChannelImpl& impl, std::shared_ptr<uint8_t> tmpBuffer) {
            TP_VLOG(5) << "Channel " << impl.id_
                       << " is done allocating temporary memory for chunk #"
                       << opIter->chunkId << " of " << opIter->numChunks
                       << " for buffer #" << opIter->bufferSequenceNumber;
            opIter->doneAllocatingCpuStagingBuffer = true;
            if (!impl.error_) {
              opIter->tmpBuffer = std::move(tmpBuffer);
            }
            impl.chunkSendOps_.advanceOperation(opIter);
          }));
}

void ChannelImpl::writeReadyToSend(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_
             << " is sending ready-to-send notification for chunk #"
             << op.chunkId << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber;
  connection_->write(
      nullptr,
      0,
      callbackWrapper_([bufferSequenceNumber{op.bufferSequenceNumber},
                        chunkId{op.chunkId},
                        numChunks{op.numChunks}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " is done sending ready-to-send notification for chunk #"
                   << chunkId << " of " << numChunks << " for buffer #"
                   << bufferSequenceNumber;
      }));
}

void ChannelImpl::copyFromGpuToCpu(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  TP_VLOG(5) << "Channel " << id_ << " is copying chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber << " from CUDA device to CPU";
  cudaCopy(
      op.tmpBuffer.get(),
      op.devicePtr,
      op.length,
      op.deviceIdx,
      op.stream,
      callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done copying chunk #"
                   << opIter->chunkId << " of " << opIter->numChunks
                   << " for buffer #" << opIter->bufferSequenceNumber
                   << " from CUDA device to CPU";
        opIter->doneCopyingFromGpuToCpu = true;
        impl.chunkSendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::sendCpuBuffer(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is sending chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber << " through CPU channel";

  cpuChannel_->send(
      CpuBuffer{.ptr = op.isCpuBuffer ? op.devicePtr : op.tmpBuffer.get()},
      op.length,
      callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " is done sending chunk #"
                   << opIter->chunkId << " of " << opIter->numChunks
                   << " for buffer #" << opIter->bufferSequenceNumber
                   << " through CPU channel";
        opIter->doneSendingCpuBuffer = true;
        impl.chunkSendOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::callSendCallback(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  if (op.callback) {
    op.callback(error_);
    // Reset callback to release the resources it was holding.
    op.callback = nullptr;
  }
}

void ChannelImpl::returnSendCpuBuffer(ChunkSendOpIter opIter) {
  ChunkSendOperation& op = *opIter;

  // The pointer's deleter will return the buffer to the allocator.
  op.tmpBuffer = nullptr;
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

  const Device device = buffer.device();
  const size_t chunkLength = kSlotSize;
  const size_t numChunks = ceilOfRatio(length, chunkLength);
  for (size_t offset = 0; offset < length; offset += chunkLength) {
    ChunkRecvOpIter opIter =
        chunkRecvOps_.emplaceBack(nextChunkBeingReceived_++);
    ChunkRecvOperation& op = *opIter;
    op.bufferSequenceNumber = sequenceNumber;
    op.chunkId = offset / chunkLength;
    op.numChunks = numChunks;
    op.length = std::min(length - offset, chunkLength);
    // Operations are processed in order, so we can afford to trigger the
    // callback once the last operation is done.
    if (op.chunkId == numChunks - 1) {
      op.callback = std::move(callback);
    }

    if (device.type == kCpuDeviceType) {
      op.isCpuBuffer = true;
      op.devicePtr =
          static_cast<uint8_t*>(buffer.unwrap<CpuBuffer>().ptr) + offset;
    } else if (device.type == kCudaDeviceType) {
      op.isCpuBuffer = false;
      op.devicePtr =
          static_cast<uint8_t*>(buffer.unwrap<CudaBuffer>().ptr) + offset;
      op.stream = buffer.unwrap<CudaBuffer>().stream;
      op.deviceIdx = device.index;
    } else {
      TP_THROW_ASSERT() << "Unexpected device type: " << device.type;
    }

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
      /*cond=*/error_ &&
          prevOpState >=
              ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU_AND_INVOKED_CALLBACK,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on control connection.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::UNINITIALIZED,
      /*to=*/ChunkRecvOperation::READING_READY_TO_SEND,
      /*cond=*/!error_ &&
          prevOpState >= ChunkRecvOperation::READING_READY_TO_SEND,
      /*actions=*/{&ChannelImpl::readReadyToSend});

  // See above for why this needs to go after previous op.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::READING_READY_TO_SEND,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/error_ && op.doneReadingReadyToSend &&
          prevOpState >=
              ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU_AND_INVOKED_CALLBACK,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of recv calls on CPU channel.
  // This operation shortcuts allocating staging memory when receiving directly
  // on CPU.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::READING_READY_TO_SEND,
      /*to=*/ChunkRecvOperation::RECEIVING_CPU_BUFFER,
      /*cond=*/!error_ && op.doneReadingReadyToSend && op.isCpuBuffer &&
          prevOpState >= ChunkRecvOperation::RECEIVING_CPU_BUFFER,
      /*actions=*/{&ChannelImpl::receiveCpuBuffer});

  // Needs to go after previous op to ensure later operations are not holding
  // staging buffers while earlier ones are still blocked waiting for them,
  // because the staging buffer will only be returned to the allocator once the
  // operation is destroyed, but this won't happen until earlier operations have
  // completed, and if they are blocked waiting for buffers we may deadlock.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::READING_READY_TO_SEND,
      /*to=*/ChunkRecvOperation::ALLOCATING_CPU_BUFFER,
      /*cond=*/!error_ && op.doneReadingReadyToSend && !op.isCpuBuffer &&
          prevOpState >= ChunkRecvOperation::ALLOCATING_CPU_BUFFER,
      /*actions=*/{&ChannelImpl::allocateRecvCpuBuffer});

  // See above for why this needs to go after previous op.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::ALLOCATING_CPU_BUFFER,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/error_ && op.doneAllocatingCpuStagingBuffer &&
          prevOpState >=
              ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU_AND_INVOKED_CALLBACK,
      /*actions=*/
      {&ChannelImpl::callRecvCallback, &ChannelImpl::returnRecvCpuBuffer});

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of recv calls on CPU channel.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::ALLOCATING_CPU_BUFFER,
      /*to=*/ChunkRecvOperation::RECEIVING_CPU_BUFFER,
      /*cond=*/!error_ && op.doneAllocatingCpuStagingBuffer &&
          prevOpState >= ChunkRecvOperation::RECEIVING_CPU_BUFFER,
      /*actions=*/{&ChannelImpl::receiveCpuBuffer});

  // See above for why this needs to go after previous op.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::RECEIVING_CPU_BUFFER,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/error_ && op.doneReceivingCpuBuffer && !op.isCpuBuffer &&
          prevOpState >=
              ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU_AND_INVOKED_CALLBACK,
      /*actions=*/
      {&ChannelImpl::callRecvCallback, &ChannelImpl::returnRecvCpuBuffer});

  // This transition shortcuts the copy to GPU when receiving on CPU memory.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::RECEIVING_CPU_BUFFER,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/op.doneReceivingCpuBuffer && op.isCpuBuffer,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::RECEIVING_CPU_BUFFER,
      /*to=*/ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU,
      /*cond=*/!error_ && op.doneReceivingCpuBuffer && !op.isCpuBuffer,
      /*actions=*/{&ChannelImpl::copyFromCpuToGpu});

  // See above for why this needs to go after previous op.
  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU,
      /*to=*/ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU_AND_INVOKED_CALLBACK,
      /*cond=*/prevOpState >=
          ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU_AND_INVOKED_CALLBACK,
      /*actions=*/{&ChannelImpl::callRecvCallback});

  chunkRecvOps_.attemptTransition(
      opIter,
      /*from=*/ChunkRecvOperation::COPYING_FROM_CPU_TO_GPU_AND_INVOKED_CALLBACK,
      /*to=*/ChunkRecvOperation::FINISHED,
      /*cond=*/op.doneCopyingFromCpuToGpu,
      /*actions=*/{&ChannelImpl::returnRecvCpuBuffer});
}

void ChannelImpl::readReadyToSend(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_
             << " is reading ready-to-send notification for chunk #"
             << op.chunkId << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber;
  connection_->read(callbackWrapper_(
      [opIter](
          ChannelImpl& impl, const void* /* unused */, size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " is done reading ready-to-send notification for chunk #"
                   << opIter->chunkId << " of " << opIter->numChunks
                   << " for buffer #" << opIter->bufferSequenceNumber;
        opIter->doneReadingReadyToSend = true;
        impl.chunkRecvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::allocateRecvCpuBuffer(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(5) << "Channel " << id_
             << " is allocating temporary memory for chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber;
  Allocator& cudaHostAllocator =
      context_->getCudaHostRecvAllocator(op.deviceIdx);
  cudaHostAllocator.alloc(
      op.length,
      callbackWrapper_(
          [opIter](
              ChannelImpl& impl, std::shared_ptr<uint8_t> tmpBuffer) mutable {
            TP_VLOG(5) << "Channel " << impl.id_
                       << " is done allocating temporary memory for chunk #"
                       << opIter->chunkId << " of " << opIter->numChunks
                       << " for buffer #" << opIter->bufferSequenceNumber;
            opIter->doneAllocatingCpuStagingBuffer = true;
            if (!impl.error_) {
              opIter->tmpBuffer = std::move(tmpBuffer);
            }
            impl.chunkRecvOps_.advanceOperation(opIter);
          }));
}

void ChannelImpl::receiveCpuBuffer(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(6) << "Channel " << id_ << " is sending chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber << " through CPU channel";
  cpuChannel_->recv(
      CpuBuffer{.ptr = op.isCpuBuffer ? op.devicePtr : op.tmpBuffer.get()},
      op.length,
      callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " is done sending chunk #"
                   << opIter->chunkId << " of " << opIter->numChunks
                   << " for buffer #" << opIter->bufferSequenceNumber
                   << " through CPU channel";
        opIter->doneReceivingCpuBuffer = true;
        impl.chunkRecvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::copyFromCpuToGpu(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  TP_VLOG(5) << "Channel " << id_ << " is copying chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #"
             << op.bufferSequenceNumber << " from CPU to CUDA device";
  cudaCopy(
      op.devicePtr,
      op.tmpBuffer.get(),
      op.length,
      op.deviceIdx,
      op.stream,
      callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done copying chunk #"
                   << opIter->chunkId << " of " << opIter->numChunks
                   << " for buffer #" << opIter->bufferSequenceNumber
                   << " from CPU to CUDA device";
        opIter->doneCopyingFromCpuToGpu = true;
        impl.chunkRecvOps_.advanceOperation(opIter);
      }));
}

void ChannelImpl::callRecvCallback(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  if (op.callback) {
    op.callback(error_);
    // Reset callback to release the resources it was holding.
    op.callback = nullptr;
  }
}

void ChannelImpl::returnRecvCpuBuffer(ChunkRecvOpIter opIter) {
  ChunkRecvOperation& op = *opIter;

  // The pointer's deleter will return the buffer to the allocator.
  op.tmpBuffer = nullptr;
}

void ChannelImpl::setIdImpl() {
  cpuChannel_->setId(id_ + ".cpu");
}

void ChannelImpl::handleErrorImpl() {
  chunkSendOps_.advanceAllOperations();
  chunkRecvOps_.advanceAllOperations();

  connection_->close();
  cpuChannel_->close();

  context_->unenroll(*this);
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
