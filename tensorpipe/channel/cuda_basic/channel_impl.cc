/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <tensorpipe/channel/cuda_basic/context_impl.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection,
    std::shared_ptr<CpuChannel> cpuChannel,
    CudaLoop& cudaLoop,
    CudaHostAllocator& cudaHostAllocator)
    : ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)),
      cpuChannel_(std::move(cpuChannel)),
      cudaLoop_(cudaLoop),
      cudaHostAllocator_(cudaHostAllocator) {}

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
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  const size_t chunkLength = cudaHostAllocator_.getChunkLength();
  const size_t numChunks = (buffer.length + chunkLength - 1) / chunkLength;
  for (size_t offset = 0; offset < buffer.length; offset += chunkLength) {
    sendOperations_.emplace_back();
    Operation& op = sendOperations_.back();
    op.sequenceNumber = sequenceNumber;
    op.chunkId = offset / chunkLength;
    op.numChunks = numChunks;
    op.stream = buffer.stream;
    op.cudaPtr = static_cast<uint8_t*>(buffer.ptr) + offset;
    op.length = std::min(buffer.length - offset, chunkLength);
    // Operations are processed in order, so we can afford to trigger the
    // callback once the last operation is done.
    if (op.chunkId == numChunks - 1) {
      op.callback = std::move(callback);
    }

    TP_VLOG(5) << "Channel " << id_
               << " is allocating temporary memory for chunk #" << op.chunkId
               << " of " << op.numChunks << " for buffer #"
               << op.sequenceNumber;
    cudaHostAllocator_.alloc(
        op.length,
        callbackWrapper_(
            [&op](ChannelImpl& impl, std::shared_ptr<uint8_t[]> tmpBuffer) {
              TP_VLOG(5) << "Channel " << impl.id_
                         << " is done allocating temporary memory for chunk #"
                         << op.chunkId << " of " << op.numChunks
                         << " for buffer #" << op.sequenceNumber;
              op.tmpBuffer = std::move(tmpBuffer);
              impl.onSendOpReadyForCopy(op);
            }));
  }

  descriptorCallback(error_, std::string());
}

void ChannelImpl::onSendOpReadyForCopy(Operation& op) {
  if (error_) {
    op.done = true;
    onSendOpDone();
    return;
  }

  TP_VLOG(5) << "Channel " << id_ << " is copying chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #" << op.sequenceNumber
             << " from CUDA device to CPU";
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), op.cudaPtr);
  cudaCopy(
      op.tmpBuffer.get(),
      op.cudaPtr,
      op.length,
      deviceIdx,
      op.stream,
      callbackWrapper_([&op](ChannelImpl& impl) {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done copying chunk #"
                   << op.chunkId << " of " << op.numChunks << " for buffer #"
                   << op.sequenceNumber << " from CUDA device to CPU";
        op.done = true;
        impl.onSendOpDone();
      }));
}

void ChannelImpl::sendChunkThroughCpuChannel(Operation op) {
  TP_VLOG(6) << "Channel " << id_ << " is sending chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #" << op.sequenceNumber
             << " through CPU channel";
  CpuBuffer cpuBuffer{op.tmpBuffer.get(), op.length};
  // FIXME: Avoid copying the op twice.
  cpuChannel_->send(
      cpuBuffer,
      callbackWrapper_([op](ChannelImpl& impl, std::string descriptor) mutable {
        if (impl.error_) {
          return;
        }
        impl.sendChunkDescriptor(std::move(op), std::move(descriptor));
      }),
      callbackWrapper_([op](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " is done sending chunk #"
                   << op.chunkId << " of " << op.numChunks << " for buffer #"
                   << op.sequenceNumber << " through CPU channel";
      }));
}

void ChannelImpl::sendChunkDescriptor(Operation op, std::string descriptor) {
  auto nopHolderOut = std::make_shared<NopHolder<std::string>>();
  nopHolderOut->getObject() = std::move(descriptor);
  TP_VLOG(6) << "Channel " << id_ << " is sending descriptor for chunk #"
             << op.chunkId << " of " << op.numChunks << " for buffer #"
             << op.sequenceNumber;
  connection_->write(
      *nopHolderOut,
      callbackWrapper_([op{std::move(op)}, nopHolderOut](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " is done sending descriptor for chunk #" << op.chunkId
                   << " of " << op.numChunks << " for buffer #"
                   << op.sequenceNumber;
      }));
}

void ChannelImpl::onSendOpDone() {
  while (!sendOperations_.empty()) {
    auto& op = sendOperations_.front();
    if (!op.done) {
      break;
    }
    if (op.callback) {
      op.callback(error_);
    }
    if (!error_) {
      sendChunkThroughCpuChannel(std::move(op));
    }
    sendOperations_.pop_front();
  }

  if (error_) {
    tryCleanup();
  }
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  const size_t chunkLength = cudaHostAllocator_.getChunkLength();
  const size_t numChunks = (buffer.length + chunkLength - 1) / chunkLength;
  for (size_t offset = 0; offset < buffer.length; offset += chunkLength) {
    recvOperations_.emplace_back();
    Operation& op = recvOperations_.back();
    op.sequenceNumber = sequenceNumber;
    op.chunkId = offset / chunkLength;
    op.numChunks = numChunks;
    op.stream = buffer.stream;
    op.cudaPtr = static_cast<uint8_t*>(buffer.ptr) + offset;
    op.length = std::min(buffer.length - offset, chunkLength);
    // Operations are processed in order, so we can afford to trigger the
    // callback once the last operation is done.
    if (op.chunkId == numChunks - 1) {
      op.callback = std::move(callback);
    }

    TP_VLOG(6) << "Channel " << id_ << " is reading descriptor for chunk #"
               << op.chunkId << " of " << op.numChunks << " for buffer #"
               << op.sequenceNumber;
    auto nopHolderIn = std::make_shared<NopHolder<std::string>>();
    connection_->read(
        *nopHolderIn,
        callbackWrapper_([nopHolderIn, &op](ChannelImpl& impl) mutable {
          TP_VLOG(6) << "Channel " << impl.id_
                     << " is done reading descriptor for chunk #" << op.chunkId
                     << " of " << op.numChunks << " for buffer #"
                     << op.sequenceNumber;
          std::string& descriptor = nopHolderIn->getObject();
          impl.onRecvOpReadDescriptor(op, std::move(descriptor));
        }));
  }
}

void ChannelImpl::onRecvOpReadDescriptor(
    Operation& op,
    std::string descriptor) {
  if (error_) {
    op.done = true;
    onRecvOpDone();
    return;
  }

  TP_VLOG(5) << "Channel " << id_
             << " is allocating temporary memory for chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #" << op.sequenceNumber;
  cudaHostAllocator_.alloc(
      op.length,
      callbackWrapper_(
          [&op, descriptor{std::move(descriptor)}](
              ChannelImpl& impl, std::shared_ptr<uint8_t[]> tmpBuffer) mutable {
            TP_VLOG(5) << "Channel " << impl.id_
                       << " is done allocating temporary memory for chunk #"
                       << op.chunkId << " of " << op.numChunks
                       << " for buffer #" << op.sequenceNumber;
            op.tmpBuffer = std::move(tmpBuffer);
            impl.onRecvOpReadyForRecv(op, std::move(descriptor));
          }));
}

void ChannelImpl::onRecvOpReadyForRecv(Operation& op, std::string descriptor) {
  if (error_) {
    op.done = true;
    onRecvOpDone();
    return;
  }

  TP_VLOG(6) << "Channel " << id_ << " is sending chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #" << op.sequenceNumber
             << " through CPU channel";
  cpuChannel_->recv(
      std::move(descriptor),
      CpuBuffer{op.tmpBuffer.get(), op.length},
      callbackWrapper_([&op](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " is done sending chunk #"
                   << op.chunkId << " of " << op.numChunks << " for buffer #"
                   << op.sequenceNumber << " through CPU channel";
        impl.onRecvOpReadyForCopy(op);
      }));
}

void ChannelImpl::onRecvOpReadyForCopy(Operation& op) {
  if (error_) {
    op.done = true;
    onRecvOpDone();
    return;
  }

  TP_VLOG(5) << "Channel " << id_ << " is copying chunk #" << op.chunkId
             << " of " << op.numChunks << " for buffer #" << op.sequenceNumber
             << " from CPU to CUDA device";
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), op.cudaPtr);
  cudaCopy(
      op.cudaPtr,
      op.tmpBuffer.get(),
      op.length,
      deviceIdx,
      op.stream,
      callbackWrapper_([&op](ChannelImpl& impl) {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done copying chunk #"
                   << op.chunkId << " of " << op.numChunks << " for buffer #"
                   << op.sequenceNumber << " from CPU to CUDA device";
        op.done = true;
        impl.onRecvOpDone();
      }));
}

void ChannelImpl::onRecvOpDone() {
  while (!recvOperations_.empty()) {
    auto& op = recvOperations_.front();
    if (!op.done) {
      break;
    }
    if (op.callback) {
      op.callback(error_);
    }
    recvOperations_.pop_front();
  }

  if (error_) {
    tryCleanup();
  }
}

void ChannelImpl::setIdImpl() {
  cpuChannel_->setId(id_ + ".cpu");
}

void ChannelImpl::tryCleanup() {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK(error_);

  if (sendOperations_.empty() && recvOperations_.empty()) {
    cleanup();
  } else {
    TP_VLOG(5) << "Channel " << id_
               << " cannot proceed with cleanup because it has "
               << sendOperations_.size() << " pending send operations and "
               << recvOperations_.size() << " pending recv operations";
  }
}

void ChannelImpl::cleanup() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(5) << "Channel " << id_ << " is cleaning up";

  context_->unenroll(*this);
}

void ChannelImpl::handleErrorImpl() {
  cpuChannel_->close();

  tryCleanup();
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
