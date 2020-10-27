/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_basic/channel.h>

#include <algorithm>
#include <list>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/cuda.h>
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
      std::shared_ptr<Context::PrivateIface>,
      std::shared_ptr<CpuChannel>,
      std::string);

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
  OnDemandLoop loop_;

  void sendFromLoop_(
      CudaBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  // Receive memory region from peer.
  void recvFromLoop_(
      TDescriptor descriptor,
      CudaBuffer buffer,
      TRecvCallback callback);

  void setIdFromLoop_(std::string id);

  void initFromLoop_();

  void closeFromLoop_();

  void setError_(Error error);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError_();

  // Proxy static method for cudaStreamAddCallback(), which does not accept
  // lambdas.
  static void CUDART_CB invokeCudaCallback_(cudaStream_t, cudaError_t, void*);

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<CpuChannel> cpuChannel_;
  std::unordered_map<uint64_t, std::function<void(cudaError_t)>>
      cudaSendCallbacks_;
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

  // Helpers to prepare callbacks from transports
  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, this->loop_};
  EagerCallbackWrapper<Impl> eagerCallbackWrapper_{*this, this->loop_};

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::LazyCallbackWrapper;
  template <typename T>
  friend class tensorpipe::EagerCallbackWrapper;
};

Channel::Channel(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<CpuChannel> cpuChannel,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(cpuChannel),
          std::move(id))) {
  impl_->init();
}

Channel::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<CpuChannel> cpuChannel,
    std::string id)
    : context_(std::move(context)),
      cpuChannel_(std::move(cpuChannel)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

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
    sendFromLoop_(buffer, std::move(descriptorCallback), std::move(callback));
  });
}

// Send memory region to peer.
void Channel::Impl::sendFromLoop_(
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_DCHECK(loop_.inLoop());

  const uint64_t sequenceNumber = nextTensorBeingSent_++;
  TP_VLOG(4) << "Channel " << id_ << " received a send request (#"
             << sequenceNumber << ")";

  // Using a shared_ptr instead of unique_ptr because if the lambda captures a
  // unique_ptr then it becomes non-copyable, which prevents it from being
  // converted to a function. In C++20 use std::make_shared<uint8_t[]>(len).
  //
  // Note: this is a std::shared_ptr<uint8_t[]> semantically. A shared_ptr
  // with array type is supported in C++17 and higher.
  //
  auto tmpBuffer = std::shared_ptr<uint8_t>(
      new uint8_t[buffer.length], std::default_delete<uint8_t[]>());
  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};

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

  callback = [this,
              sequenceNumber,
              tmpBuffer{std::move(tmpBuffer)},
              callback{std::move(callback)}](const Error& error) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG(4) << "Channel " << id_ << " is calling a send callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a send callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  TP_CUDA_CHECK(cudaMemcpyAsync(
      cpuBuffer.ptr,
      buffer.ptr,
      buffer.length,
      cudaMemcpyDeviceToHost,
      buffer.stream));

  TP_DCHECK(
      cudaSendCallbacks_.find(sequenceNumber) == cudaSendCallbacks_.end());
  cudaSendCallbacks_[sequenceNumber] =
      [this,
       sequenceNumber,
       cpuBuffer,
       descriptorCallback{std::move(descriptorCallback)},
       callback{std::move(callback)}](cudaError_t cudaError) {
        TP_CUDA_CHECK(cudaError);
        TP_VLOG(6) << "Channel " << id_ << " is writing payload (#"
                   << sequenceNumber << ")";
        cpuChannel_->send(
            cpuBuffer, std::move(descriptorCallback), std::move(callback));

        // Deleting the lambda from within the lambda. It looks shady but it is
        // fine.
        cudaSendCallbacks_.erase(sequenceNumber);
      };
  TP_CUDA_CHECK(cudaStreamAddCallback(
      buffer.stream,
      invokeCudaCallback_,
      &cudaSendCallbacks_[sequenceNumber],
      0));
}

void Channel::Impl::invokeCudaCallback_(
    cudaStream_t /* stream */,
    cudaError_t status,
    void* callbackPtr) {
  auto& callback =
      *reinterpret_cast<std::function<void(cudaError_t)>*>(callbackPtr);

  callback(status);
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
    recvFromLoop_(std::move(descriptor), buffer, std::move(callback));
  });
}

void Channel::Impl::recvFromLoop_(
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  TP_DCHECK(loop_.inLoop());

  const uint64_t sequenceNumber = nextTensorBeingReceived_++;
  TP_VLOG(4) << "Channel " << id_ << " received a recv request (#"
             << sequenceNumber << ")";

  // Using a shared_ptr instead of unique_ptr because if the lambda captures a
  // unique_ptr then it becomes non-copyable, which prevents it from being
  // converted to a function. In C++20 use std::make_shared<uint8_t[]>(len).
  //
  // Note: this is a std::shared_ptr<uint8_t[]> semantically. A shared_ptr
  // with array type is supported in C++17 and higher.
  //
  auto tmpBuffer = std::shared_ptr<uint8_t>(
      new uint8_t[buffer.length], std::default_delete<uint8_t[]>());
  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};
  callback = [this,
              sequenceNumber,
              buffer,
              tmpBuffer{std::move(tmpBuffer)},
              callback{std::move(callback)}](const Error& error) {
    TP_CUDA_CHECK(cudaMemcpyAsync(
        buffer.ptr,
        tmpBuffer.get(),
        buffer.length,
        cudaMemcpyHostToDevice,
        buffer.stream));
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG(4) << "Channel " << id_ << " is calling a recv callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a recv callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    callback(error_);
    return;
  }

  TP_DCHECK_EQ(descriptor, std::string());

  TP_VLOG(6) << "Channel " << id_ << " is reading payload (#" << sequenceNumber
             << ")";
  cpuChannel_->recv(std::move(descriptor), cpuBuffer, std::move(callback));
}

void Channel::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
}

void Channel::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Channel::Impl::setId(std::string id) {
  loop_.deferToLoop(
      [this, id{std::move(id)}]() mutable { setIdFromLoop_(std::move(id)); });
}

void Channel::Impl::setIdFromLoop_(std::string id) {
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
  loop_.deferToLoop([this]() { closeFromLoop_(); });
}

void Channel::Impl::closeFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(4) << "Channel " << id_ << " is closing";
  setError_(TP_CREATE_ERROR(ChannelClosedError));
}

void Channel::Impl::setError_(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError_();
}

void Channel::Impl::handleError_() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(5) << "Channel " << id_ << " is handling error " << error_.what();

  cpuChannel_->close();

  // Since we cannot un-schedule a callback from a CUDA stream, we have to
  // ensure they have all been processed before destroying.
  while (!cudaSendCallbacks_.empty())
    ;
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
