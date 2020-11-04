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

namespace {

std::shared_ptr<uint8_t> makeCudaPinnedBuffer(size_t length) {
  void* ptr;
  TP_CUDA_CHECK(cudaMallocHost(&ptr, length));
  return std::shared_ptr<uint8_t>(
      reinterpret_cast<uint8_t*>(ptr),
      [](uint8_t* ptr) { TP_CUDA_CHECK(cudaFreeHost(ptr)); });
}

} // namespace

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
  static void CUDART_CB signalOperationReady_(cudaStream_t, cudaError_t, void*);
  void processOperations_();

  std::thread opThread_;
  std::mutex opMutex_;
  std::condition_variable opCondVar_;

  struct Operation {
    std::function<void(cudaError_t)> process;
    std::function<void(cudaError_t)> signalReady;
    bool ready{false};
    cudaError_t cudaError;
  };

  std::deque<Operation> sendOperations_;
  std::deque<Operation> recvOperations_;

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<CpuChannel> cpuChannel_;
  std::unordered_map<uint64_t, std::function<void(cudaError_t)>>
      cudaSendCallbacks_;
  std::unordered_map<uint64_t, std::function<void(cudaError_t)>>
      cudaRecvCallbacks_;
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

void Channel::Impl::processOperations_() {
  setThreadName("TP_CUDA_BASIC_loop");
  static int sendOpId = 0;
  static int recvOpId = 0;

  for (;;) {
    std::unique_lock<std::mutex> lock(opMutex_);
    opCondVar_.wait(lock);

    while (!sendOperations_.empty()) {
      auto& op = sendOperations_.front();
      if (!op.ready) {
        break;
      }

      op.process(op.cudaError);
      sendOperations_.pop_front();
    }

    while (!recvOperations_.empty()) {
      auto& op = recvOperations_.front();
      if (!op.ready) {
        break;
      }
      op.process(op.cudaError);
      recvOperations_.pop_front();
      recvOpId++;
    }

    if (error_ && sendOperations_.empty() && recvOperations_.empty()) {
      break;
    }
  }
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

  if (error_) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  TP_VLOG(4) << "Channel " << id_
             << " is copying buffer from CUDA device to CPU";
  auto tmpBuffer = makeCudaPinnedBuffer(buffer.length);
  TP_CUDA_CHECK(cudaMemcpyAsync(
      tmpBuffer.get(),
      buffer.ptr,
      buffer.length,
      cudaMemcpyDeviceToHost,
      buffer.stream));

  sendOperations_.emplace_back();
  auto& op = sendOperations_.back();
  op.process = [this,
                sequenceNumber,
                buffer,
                tmpBuffer{std::move(tmpBuffer)},
                descriptorCallback{std::move(descriptorCallback)},
                callback{std::move(callback)}](cudaError_t cudaError) {
    TP_VLOG(4) << "Channel " << id_
               << " is done copying buffer from CUDA device to CPU";
    TP_CUDA_CHECK(cudaError);
    CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};
    cpuChannel_->send(
        cpuBuffer,
        std::move(descriptorCallback),
        [tmpBuffer{std::move(tmpBuffer)}, callback{std::move(callback)}](
            const Error& error) { callback(error); });
  };
  op.signalReady = [this, &op, sequenceNumber](cudaError_t cudaError) {
    std::unique_lock<std::mutex> lock(opMutex_);
    op.cudaError = cudaError;
    op.ready = true;
    opCondVar_.notify_all();
  };
  TP_CUDA_CHECK(cudaStreamAddCallback(
      buffer.stream, signalOperationReady_, &op.signalReady, 0));
}

void Channel::Impl::signalOperationReady_(
    cudaStream_t /* stream */,
    cudaError_t status,
    void* opReadyPtr) {
  auto& opReady =
      *reinterpret_cast<std::function<void(cudaError_t)>*>(opReadyPtr);
  opReady(status);
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

  if (error_) {
    callback(error_);
    return;
  }

  auto tmpBuffer = makeCudaPinnedBuffer(buffer.length);

  TP_DCHECK_EQ(descriptor, std::string());
  std::unique_lock<std::mutex> lock(opMutex_);

  recvOperations_.emplace_back();
  auto& op = recvOperations_.back();

  CpuBuffer cpuBuffer{tmpBuffer.get(), buffer.length};
  callback = [this,
              &op,
              sequenceNumber,
              buffer,
              tmpBuffer{std::move(tmpBuffer)},
              callback{std::move(callback)}](const Error& error) {
    TP_VLOG(4) << "Channel " << id_
               << " is copying buffer from CPU to CUDA device";
    TP_CUDA_CHECK(cudaMemcpyAsync(
        buffer.ptr,
        tmpBuffer.get(),
        buffer.length,
        HostToDevice,
        buffer.stream));

    callback(error_);

    op.process = [this, sequenceNumber, tmpBuffer{std::move(tmpBuffer)}](
                     cudaError_t cudaError) {
      TP_VLOG(4) << "Channel " << id_
                 << " is done copying buffer from CPU to CUDA device";
      TP_CUDA_CHECK(cudaError);
    };
    op.signalReady = [this, &op, sequenceNumber](cudaError_t cudaError) {
      std::unique_lock<std::mutex> lock(opMutex_);
      op.cudaError = cudaError;
      op.ready = true;
      opCondVar_.notify_all();
    };
    TP_CUDA_CHECK(cudaStreamAddCallback(
        buffer.stream, signalOperationReady_, &op.signalReady, 0));
  };

  cpuChannel_->recv(std::move(descriptor), cpuBuffer, std::move(callback));
}

void Channel::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
  opThread_ = std::thread([this]() { processOperations_(); });
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
  {
    std::unique_lock<std::mutex> lock(opMutex_);
    opCondVar_.notify_all();
  }
  opThread_.join();
}

} // namespace cuda_basic
} // namespace channel
} // namespace tensorpipe
