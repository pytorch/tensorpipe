/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_basic/channel.h>

#include <algorithm>
#include <condition_variable>
#include <list>
#include <mutex>

#include <tensorpipe/channel/cuda_basic/context_impl.h>
#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_loop.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_basic {

class Channel::Impl : public std::enable_shared_from_this<Channel::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      std::shared_ptr<CpuChannel> cpuChannel,
      CudaLoop& cudaLoop,
      std::string id);

  // Called by the channel's constructor.
  void init();

  void send(
      CudaBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  void recv(TDescriptor descriptor, CudaBuffer buffer, TRecvCallback callback);

  // Tell the channel what its identifier is.
  void setId(std::string id);

  void close();

 private:
  OnDemandDeferredExecutor loop_;

  void initFromLoop();

  // Send memory region to peer.
  void sendFromLoop(
      CudaBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  void onTempBufferReadyForSend(
      CudaBuffer buffer,
      CudaPinnedBuffer tmpBuffer,
      TDescriptorCallback descriptorCallback);

  // Receive memory region from peer.
  void recvFromLoop(
      TDescriptor descriptor,
      CudaBuffer buffer,
      TRecvCallback callback);

  void onCpuChannelRecv(
      CudaBuffer buffer,
      CudaPinnedBuffer tmpBuffer,
      TRecvCallback callback);

  void closeFromLoop();

  void setError(Error error);

  void setIdFromLoop(std::string id);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<CpuChannel> cpuChannel_;
  CudaLoop& cudaLoop_;
  Error error_{Error::kSuccess};

  ClosingReceiver closingReceiver_;

  // Increasing identifier for send operations.
  uint64_t nextTensorBeingSent_{0};

  // Increasing identifier for recv operations.
  uint64_t nextTensorBeingReceived_{0};

  // An identifier for the channel, composed of the identifier for the context,
  // combined with an increasing sequence number. It will only be used for
  // logging and debugging purposes.
  std::string id_;

  EagerCallbackWrapper<Impl> eagerCallbackWrapper_{*this, this->loop_};

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::EagerCallbackWrapper;
};

Channel::Channel(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<CpuChannel> cpuChannel,
    CudaLoop& cudaLoop,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(cpuChannel),
          cudaLoop,
          std::move(id))) {
  impl_->init();
}

Channel::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<CpuChannel> cpuChannel,
    CudaLoop& cudaLoop,
    std::string id)
    : context_(std::move(context)),
      cpuChannel_(std::move(cpuChannel)),
      cudaLoop_(cudaLoop),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

void Channel::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop(); });
}

void Channel::Impl::initFromLoop() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
}

void Channel::send(
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  impl_->send(buffer, std::move(descriptorCallback), std::move(callback));
}

void Channel::Impl::send(
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  loop_.deferToLoop([this,
                     buffer,
                     descriptorCallback{std::move(descriptorCallback)},
                     callback{std::move(callback)}]() mutable {
    sendFromLoop(buffer, std::move(descriptorCallback), std::move(callback));
  });
}

// Send memory region to peer.
void Channel::Impl::sendFromLoop(
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_DCHECK(loop_.inLoop());

  const uint64_t sequenceNumber = nextTensorBeingSent_++;
  TP_VLOG(4) << "Channel " << id_ << " received a send request (#"
             << sequenceNumber << ")";

  descriptorCallback = [this,
                        sequenceNumber,
                        descriptorCallback{std::move(descriptorCallback)}](
                           const Error& error, TDescriptor descriptor) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG(4) << "Channel " << id_ << " is calling a descriptor callback (#"
               << sequenceNumber << ")";
    descriptorCallback(error, std::move(descriptor));
    TP_VLOG(4) << "Channel " << id_ << " done calling a descriptor callback (#"
               << sequenceNumber << ")";
  };

  callback = [this, sequenceNumber, callback{std::move(callback)}](
                 const Error& error) {
    TP_VLOG(4) << "Channel " << id_ << " is calling a send callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a send callback (#"
               << sequenceNumber << ")";
  };

  if (error_ || buffer.length == 0) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  TP_VLOG(5) << "Channel " << id_
             << " is copying buffer from CUDA device to CPU";
  auto tmpBuffer = makeCudaPinnedBuffer(buffer.length);
  TP_CUDA_CHECK(cudaMemcpyAsync(
      tmpBuffer.get(),
      buffer.ptr,
      buffer.length,
      cudaMemcpyDeviceToHost,
      buffer.stream));

  cudaLoop_.addCallback(
      cudaDeviceForPointer(buffer.ptr),
      buffer.stream,
      eagerCallbackWrapper_([buffer,
                             tmpBuffer{std::move(tmpBuffer)},
                             descriptorCallback{std::move(descriptorCallback)}](
                                Impl& impl) mutable {
        impl.onTempBufferReadyForSend(
            buffer, std::move(tmpBuffer), std::move(descriptorCallback));
      }));

  callback(Error::kSuccess);
}

void Channel::Impl::onTempBufferReadyForSend(
    CudaBuffer buffer,
    CudaPinnedBuffer tmpBuffer,
    TDescriptorCallback descriptorCallback) {
  if (error_) {
    descriptorCallback(error_, std::string());
    return;
  }

  TP_VLOG(5) << "Channel " << id_
             << " is done copying buffer from CUDA device to CPU";

  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};
  // Keep tmpBuffer alive until cpuChannel_ is done sending it over.
  // TODO: This could be a lazy callback wrapper.
  auto callback =
      eagerCallbackWrapper_([tmpBuffer{std::move(tmpBuffer)}](Impl& impl) {
        TP_VLOG(5) << "Channel " << impl.id_
                   << " is done sending buffer through CPU channel";
      });
  TP_VLOG(6) << "Channel " << id_ << " is sending buffer through CPU channel";
  cpuChannel_->send(
      cpuBuffer, std::move(descriptorCallback), std::move(callback));
}

// Receive memory region from peer.
void Channel::recv(
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  impl_->recv(std::move(descriptor), buffer, std::move(callback));
}

void Channel::Impl::recv(
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  loop_.deferToLoop([this,
                     descriptor{std::move(descriptor)},
                     buffer,
                     callback{std::move(callback)}]() mutable {
    recvFromLoop(std::move(descriptor), buffer, std::move(callback));
  });
}

void Channel::Impl::recvFromLoop(
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  TP_DCHECK(loop_.inLoop());

  const uint64_t sequenceNumber = nextTensorBeingReceived_++;
  TP_VLOG(4) << "Channel " << id_ << " received a recv request (#"
             << sequenceNumber << ")";
  callback = [this, sequenceNumber, callback{std::move(callback)}](
                 const Error& error) {
    TP_VLOG(4) << "Channel " << id_ << " is calling a recv callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a recv callback (#"
               << sequenceNumber << ")";
  };

  if (error_ || buffer.length == 0) {
    callback(error_);
    return;
  }

  auto tmpBuffer = makeCudaPinnedBuffer(buffer.length);
  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};

  cpuChannel_->recv(
      std::move(descriptor),
      cpuBuffer,
      eagerCallbackWrapper_(
          [buffer,
           tmpBuffer{std::move(tmpBuffer)},
           callback{std::move(callback)}](Impl& impl) mutable {
            impl.onCpuChannelRecv(
                buffer, std::move(tmpBuffer), std::move(callback));
          }));
}

void Channel::Impl::onCpuChannelRecv(
    CudaBuffer buffer,
    CudaPinnedBuffer tmpBuffer,
    TRecvCallback callback) {
  if (error_) {
    callback(error_);
    return;
  }

  TP_VLOG(5) << "Channel " << id_
             << " is copying buffer from CPU to CUDA device";
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
      eagerCallbackWrapper_(
          [tmpBuffer{std::move(tmpBuffer)}](Impl& impl) mutable {
            TP_VLOG(5) << "Channel " << impl.id_
                       << " is done copying buffer from CPU to CUDA device";
          }));

  callback(Error::kSuccess);
}

void Channel::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Channel::Impl::setId(std::string id) {
  loop_.deferToLoop(
      [this, id{std::move(id)}]() mutable { setIdFromLoop(std::move(id)); });
}

void Channel::Impl::setIdFromLoop(std::string id) {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(4) << "Channel " << id_ << " was renamed to " << id;
  id_ = std::move(id);
  cpuChannel_->setId(id_ + ".cpu");
}

void Channel::close() {
  impl_->close();
}

Channel::~Channel() {
  close();
}

void Channel::Impl::close() {
  loop_.deferToLoop([this]() { closeFromLoop(); });
}

void Channel::Impl::closeFromLoop() {
  TP_DCHECK(loop_.inLoop());
  setError(TP_CREATE_ERROR(ChannelClosedError));
}

void Channel::Impl::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void Channel::Impl::handleError() {
  TP_DCHECK(loop_.inLoop());

  cpuChannel_->close();
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
