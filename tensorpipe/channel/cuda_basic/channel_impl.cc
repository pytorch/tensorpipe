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

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection,
    std::shared_ptr<CpuChannel> cpuChannel,
    CudaLoop& cudaLoop)
    : ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)),
      cpuChannel_(std::move(cpuChannel)),
      cudaLoop_(cudaLoop) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);

  TP_VLOG(5) << "Channel " << id_ << " is copying buffer #" << sequenceNumber
             << " from CUDA device to CPU";
  auto tmpBuffer = makeCudaPinnedBuffer(buffer.length, deviceIdx);
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaMemcpyAsync(
        tmpBuffer.get(),
        buffer.ptr,
        buffer.length,
        cudaMemcpyDeviceToHost,
        buffer.stream));
  }

  sendOperations_.emplace_back();
  auto& op = sendOperations_.back();
  op.sequenceNumber = sequenceNumber;
  op.tmpBuffer = std::move(tmpBuffer);
  op.length = buffer.length;
  op.descriptorCallback = std::move(descriptorCallback);
  op.ready = false;

  cudaLoop_.addCallback(
      cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr),
      buffer.stream,
      callbackWrapper_([&op](ChannelImpl& impl) {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done copying buffer #"
                   << op.sequenceNumber << " from CUDA device to CPU";
        op.ready = true;
        impl.onTempBufferReadyForSend();
      }));

  callback(Error::kSuccess);
}

void ChannelImpl::onTempBufferReadyForSend() {
  while (!sendOperations_.empty()) {
    auto& op = sendOperations_.front();
    if (!op.ready) {
      break;
    }

    if (error_) {
      op.descriptorCallback(error_, std::string());
    } else {
      CpuBuffer cpuBuffer{op.tmpBuffer.get(), op.length};
      // Keep tmpBuffer alive until cpuChannel_ is done sending it over.
      // TODO: This could be a lazy callback wrapper.
      auto callback = callbackWrapper_(
          [sequenceNumber{op.sequenceNumber},
           tmpBuffer{std::move(op.tmpBuffer)}](ChannelImpl& impl) {
            TP_VLOG(5) << "Channel " << impl.id_ << " is done sending buffer #"
                       << sequenceNumber << " through CPU channel";
          });
      TP_VLOG(6) << "Channel " << id_ << " is sending buffer #"
                 << op.sequenceNumber << " through CPU channel";
      cpuChannel_->send(
          cpuBuffer, std::move(op.descriptorCallback), std::move(callback));
    }

    sendOperations_.pop_front();
  }
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);

  auto tmpBuffer = makeCudaPinnedBuffer(buffer.length, deviceIdx);
  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};

  TP_VLOG(6) << "Channel " << id_ << " is receiving buffer #" << sequenceNumber
             << " through CPU channel";
  cpuChannel_->recv(
      std::move(descriptor),
      cpuBuffer,
      callbackWrapper_([sequenceNumber,
                        buffer,
                        deviceIdx,
                        tmpBuffer{std::move(tmpBuffer)},
                        callback{std::move(callback)}](
                           ChannelImpl& impl) mutable {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done receiving buffer #"
                   << sequenceNumber << " through CPU channel";
        impl.onCpuChannelRecv(
            sequenceNumber,
            buffer,
            deviceIdx,
            std::move(tmpBuffer),
            std::move(callback));
      }));
}

void ChannelImpl::onCpuChannelRecv(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    int deviceIdx,
    CudaPinnedBuffer tmpBuffer,
    TRecvCallback callback) {
  if (error_) {
    callback(error_);
    return;
  }

  TP_VLOG(5) << "Channel " << id_ << " is copying buffer #" << sequenceNumber
             << " from CPU to CUDA device";
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaMemcpyAsync(
        buffer.ptr,
        tmpBuffer.get(),
        buffer.length,
        cudaMemcpyHostToDevice,
        buffer.stream));
  }

  // Keep tmpBuffer alive until cudaMemcpyAsync is done.
  cudaLoop_.addCallback(
      cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr),
      buffer.stream,
      callbackWrapper_([sequenceNumber, tmpBuffer{std::move(tmpBuffer)}](
                           ChannelImpl& impl) mutable {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done copying buffer #"
                   << sequenceNumber << " from CPU to CUDA device";
      }));

  callback(Error::kSuccess);
}

void ChannelImpl::setIdImpl() {
  cpuChannel_->setId(id_ + ".cpu");
}

void ChannelImpl::handleErrorImpl() {
  cpuChannel_->close();

  context_->unenroll(*this);
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
