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
    std::shared_ptr<CpuChannel> cpuChannel,
    CudaLoop& cudaLoop,
    CudaPinnedBufferAllocator& cudaPinnedBufferAllocator)
    : ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      cpuChannel_(std::move(cpuChannel)),
      cudaLoop_(cudaLoop),
      cudaPinnedBufferAllocator_(cudaPinnedBufferAllocator) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_VLOG(5) << "Channel " << id_ << " is copying buffer #" << sequenceNumber
             << " from CUDA device to CPU";
  std::shared_ptr<uint8_t> tmpBuffer =
      cudaPinnedBufferAllocator_.getBuffer(buffer.length);
  TP_CUDA_CHECK(cudaMemcpyAsync(
      tmpBuffer.get(),
      buffer.ptr,
      buffer.length,
      cudaMemcpyDeviceToHost,
      buffer.stream));

  cudaLoop_.addCallback(
      cudaDeviceForPointer(buffer.ptr),
      buffer.stream,
      eagerCallbackWrapper_([sequenceNumber,
                             buffer,
                             tmpBuffer{std::move(tmpBuffer)},
                             descriptorCallback{std::move(descriptorCallback)}](
                                ChannelImpl& impl) mutable {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done copying buffer #"
                   << sequenceNumber << " from CUDA device to CPU";
        impl.onTempBufferReadyForSend(
            sequenceNumber,
            buffer,
            std::move(tmpBuffer),
            std::move(descriptorCallback));
      }));

  callback(Error::kSuccess);
}

void ChannelImpl::onTempBufferReadyForSend(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    std::shared_ptr<uint8_t> tmpBuffer,
    TDescriptorCallback descriptorCallback) {
  if (error_) {
    descriptorCallback(error_, std::string());
    return;
  }

  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};
  // Keep tmpBuffer alive until cpuChannel_ is done sending it over.
  // TODO: This could be a lazy callback wrapper.
  auto callback = eagerCallbackWrapper_(
      [sequenceNumber, tmpBuffer{std::move(tmpBuffer)}](ChannelImpl& impl) {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done sending buffer #"
                   << sequenceNumber << " through CPU channel";
      });
  TP_VLOG(6) << "Channel " << id_ << " is sending buffer #" << sequenceNumber
             << " through CPU channel";
  cpuChannel_->send(
      cpuBuffer, std::move(descriptorCallback), std::move(callback));
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  std::shared_ptr<uint8_t> tmpBuffer =
      cudaPinnedBufferAllocator_.getBuffer(buffer.length);
  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};

  TP_VLOG(6) << "Channel " << id_ << " is receiving buffer #" << sequenceNumber
             << " through CPU channel";
  cpuChannel_->recv(
      std::move(descriptor),
      cpuBuffer,
      eagerCallbackWrapper_([sequenceNumber,
                             buffer,
                             tmpBuffer{std::move(tmpBuffer)},
                             callback{std::move(callback)}](
                                ChannelImpl& impl) mutable {
        TP_VLOG(5) << "Channel " << impl.id_ << " is done receiving buffer #"
                   << sequenceNumber << " through CPU channel";
        impl.onCpuChannelRecv(
            sequenceNumber, buffer, std::move(tmpBuffer), std::move(callback));
      }));
}

void ChannelImpl::onCpuChannelRecv(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    std::shared_ptr<uint8_t> tmpBuffer,
    TRecvCallback callback) {
  if (error_) {
    callback(error_);
    return;
  }

  TP_VLOG(5) << "Channel " << id_ << " is copying buffer #" << sequenceNumber
             << " from CPU to CUDA device";
  TP_CUDA_CHECK(cudaMemcpyAsync(
      buffer.ptr,
      tmpBuffer.get(),
      buffer.length,
      cudaMemcpyHostToDevice,
      buffer.stream));

  // Keep tmpBuffer alive until cudaMemcpyAsync is done.
  cudaLoop_.addCallback(
      cudaDeviceForPointer(buffer.ptr),
      buffer.stream,
      eagerCallbackWrapper_([sequenceNumber, tmpBuffer{std::move(tmpBuffer)}](
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
