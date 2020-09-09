/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_ipc/channel.h>

#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <limits>
#include <list>

#include <cuda_runtime.h>

#include <nop/structure.h>
#include <nop/types/variant.h>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/common/system.h>

#define TP_CUDA_CHECK(a)                                                      \
  TP_THROW_ASSERT_IF(cudaSuccess != (a))                                      \
      << __TP_EXPAND_OPD(a) << " " << cudaGetErrorName(cudaPeekAtLastError()) \
      << " (" << cudaGetErrorString(cudaPeekAtLastError()) << ")"

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

namespace {

struct Descriptor {
  std::string handle;
  std::string startEvHandle;
  NOP_STRUCTURE(Descriptor, handle, startEvHandle);
};

struct Reply {
  std::string stopEvHandle;
  NOP_STRUCTURE(Reply, stopEvHandle);
};

struct Ack {
  NOP_STRUCTURE(Ack);
};

using Packet = nop::Variant<Reply, Ack>;

class CudaEvent {
 public:
  explicit CudaEvent(int device) {
    TP_CUDA_CHECK(cudaSetDevice(device));
    TP_CUDA_CHECK(cudaEventCreateWithFlags(
        &ev_, cudaEventDisableTiming | cudaEventInterprocess));
  }

  explicit CudaEvent(cudaIpcEventHandle_t handle) {
    TP_CUDA_CHECK(cudaIpcOpenEventHandle(&ev_, handle));
  }

  void record(cudaStream_t stream) {
    TP_CUDA_CHECK(cudaEventRecord(ev_, stream));
  }

  void wait(cudaStream_t stream) {
    TP_CUDA_CHECK(cudaStreamWaitEvent(stream, ev_, 0));
  }

  std::string serializedHandle() {
    cudaIpcEventHandle_t handle;
    TP_CUDA_CHECK(cudaIpcGetEventHandle(&handle, ev_));

    return std::string(reinterpret_cast<const char*>(&handle), sizeof(handle));
  }

  ~CudaEvent() {
    TP_CUDA_CHECK(cudaEventDestroy(ev_));
  }

 private:
  cudaEvent_t ev_;
};

int cudaDeviceForPointer(const void* ptr) {
  cudaPointerAttributes attrs;
  TP_CUDA_CHECK(cudaPointerGetAttributes(&attrs, ptr));
#if (CUDART_VERSION >= 10000)
  TP_DCHECK_EQ(cudaMemoryTypeDevice, attrs.type);
#else
  TP_DCHECK_EQ(cudaMemoryTypeDevice, attrs.memoryType);
#endif
  return attrs.device;
}

class SendOperation {
 public:
  uint64_t sequenceNumber;
  TSendCallback callback;

  SendOperation(
      uint64_t sequenceNumber,
      TSendCallback callback,
      const void* ptr,
      cudaStream_t stream)
      : sequenceNumber(sequenceNumber),
        callback(std::move(callback)),
        ptr_(ptr),
        stream_(stream),
        startEv_(cudaDeviceForPointer(ptr)) {
    startEv_.record(stream_);
  }

  Descriptor descriptor() {
    cudaIpcMemHandle_t handle;
    TP_CUDA_CHECK(cudaIpcGetMemHandle(&handle, const_cast<void*>(ptr_)));

    return Descriptor{
        std::string(reinterpret_cast<const char*>(&handle), sizeof(handle)),
        startEv_.serializedHandle()};
  }

  void process(const cudaIpcEventHandle_t& stopEvHandle) {
    CudaEvent stopEv(stopEvHandle);
    stopEv.wait(stream_);
  }

 private:
  const void* ptr_;
  cudaStream_t stream_;
  CudaEvent startEv_;
};

struct RecvOperation {
 public:
  uint64_t sequenceNumber;

  RecvOperation(
      uint64_t sequenceNumber,
      void* ptr,
      cudaStream_t stream,
      size_t length)
      : sequenceNumber(sequenceNumber),
        ptr_(ptr),
        stream_(stream),
        length_(length),
        stopEv_(cudaDeviceForPointer(ptr)) {}

  Reply reply() {
    return Reply{stopEv_.serializedHandle()};
  }

  void process(
      const cudaIpcEventHandle_t& startEvHandle,
      const cudaIpcMemHandle_t& remoteHandle) {
    CudaEvent startEv(startEvHandle);
    startEv.wait(stream_);

    void* remotePtr;
    TP_CUDA_CHECK(cudaIpcOpenMemHandle(
        &remotePtr, remoteHandle, cudaIpcMemLazyEnablePeerAccess));
    TP_CUDA_CHECK(cudaMemcpyAsync(
        ptr_, remotePtr, length_, cudaMemcpyDeviceToDevice, stream_));
    TP_CUDA_CHECK(cudaIpcCloseMemHandle(remotePtr));

    stopEv_.record(stream_);
  }

 private:
  void* ptr_;
  cudaStream_t stream_;
  size_t length_;
  CudaEvent stopEv_;
};

} // namespace

class Channel::Impl : public std::enable_shared_from_this<Channel::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface>,
      std::shared_ptr<transport::Connection>,
      std::string);

  // Called by the channel's constructor.
  void init();

  void send(
      const void* ptr,
      size_t length,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback,
      cudaStream_t stream);

  void recv(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback,
      cudaStream_t stream);

  // Tell the channel what its identifier is.
  void setId(std::string id);

  void close();

 private:
  OnDemandLoop loop_;

  void initFromLoop_();

  // Send memory region to peer.
  void sendFromLoop_(
      const void* ptr,
      size_t length,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback,
      cudaStream_t stream);

  // Receive memory region from peer.
  void recvFromLoop_(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback,
      cudaStream_t stream);

  void readPackets_();
  void onReply_(const Reply& nopReply);
  void onAck_();

  void closeFromLoop_();

  void setError_(Error error);

  void setIdFromLoop_(std::string id);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError_();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<transport::Connection> connection_;
  Error error_{Error::kSuccess};

  ClosingReceiver closingReceiver_;

  // Increasing identifier for send operations.
  uint64_t nextTensorBeingSent_{0};

  // Increasing identifier for recv operations.
  uint64_t nextTensorBeingReceived_{0};

  // List of alive send operations.
  std::list<SendOperation> sendOperations_;

  // List of alive recv operations.
  std::list<RecvOperation> recvOperations_;

  // An identifier for the channel, composed of the identifier for the context,
  // combined with an increasing sequence number. It will only be used for
  // logging and debugging purposes.
  std::string id_;

  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, this->loop_};

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::LazyCallbackWrapper;
};

Channel::Channel(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(connection),
          std::move(id))) {
  impl_->init();
}

Channel::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::shared_ptr<transport::Connection> connection,
    std::string id)
    : context_(std::move(context)),
      connection_(std::move(connection)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

void Channel::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Channel::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
  readPackets_();
}

void Channel::send(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  send(
      ptr,
      length,
      std::move(descriptorCallback),
      std::move(callback),
      cudaStreamDefault);
}

void Channel::send(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback,
    cudaStream_t stream) {
  impl_->send(
      ptr, length, std::move(descriptorCallback), std::move(callback), stream);
}

void Channel::Impl::send(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback,
    cudaStream_t stream) {
  loop_.deferToLoop([this,
                     ptr,
                     length,
                     stream,
                     descriptorCallback{std::move(descriptorCallback)},
                     callback{std::move(callback)}]() mutable {
    sendFromLoop_(
        ptr,
        length,
        std::move(descriptorCallback),
        std::move(callback),
        stream);
  });
}

void Channel::Impl::sendFromLoop_(
    const void* ptr,
    size_t length,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback,
    cudaStream_t stream) {
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

  if (error_ || length == 0) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  sendOperations_.emplace_back(
      sequenceNumber, std::move(callback), ptr, stream);
  auto& op = sendOperations_.back();

  NopHolder<Descriptor> nopHolder;
  nopHolder.getObject() = op.descriptor();
  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
}

// Receive memory region from peer.
void Channel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback) {
  recv(
      std::move(descriptor),
      ptr,
      length,
      std::move(callback),
      cudaStreamDefault);
}

void Channel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback,
    cudaStream_t stream) {
  impl_->recv(std::move(descriptor), ptr, length, std::move(callback), stream);
}

void Channel::Impl::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback,
    cudaStream_t stream) {
  loop_.deferToLoop([this,
                     descriptor{std::move(descriptor)},
                     ptr,
                     length,
                     stream,
                     callback{std::move(callback)}]() mutable {
    recvFromLoop_(
        std::move(descriptor), ptr, length, std::move(callback), stream);
  });
}

void Channel::Impl::recvFromLoop_(
    TDescriptor descriptor,
    void* ptr,
    size_t length,
    TRecvCallback callback,
    cudaStream_t stream) {
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

  if (error_ || length == 0) {
    callback(error_);
    return;
  }

  recvOperations_.emplace_back(sequenceNumber, ptr, stream, length);
  auto& op = recvOperations_.back();

  NopHolder<Descriptor> nopHolder;
  loadDescriptor(nopHolder, descriptor);
  Descriptor& nopDescriptor = nopHolder.getObject();
  const cudaIpcEventHandle_t* startEvHandle =
      reinterpret_cast<const cudaIpcEventHandle_t*>(
          nopDescriptor.startEvHandle.c_str());
  const cudaIpcMemHandle_t* remoteHandle =
      reinterpret_cast<const cudaIpcMemHandle_t*>(nopDescriptor.handle.c_str());

  // Perform copy.
  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#" << sequenceNumber
             << ")";

  op.process(*startEvHandle, *remoteHandle);

  TP_VLOG(6) << "Channel " << id_ << " done copying payload (#"
             << op.sequenceNumber << ")";

  callback(error_);

  // Let peer know we've completed the copy.
  TP_VLOG(6) << "Channel " << id_ << " is writing reply notification (#"
             << op.sequenceNumber << ")";
  auto nopPacketHolder = std::make_shared<NopHolder<Packet>>();
  nopPacketHolder->getObject() = op.reply();

  connection_->write(
      *nopPacketHolder,
      lazyCallbackWrapper_(
          [nopPacketHolder, sequenceNumber{op.sequenceNumber}](Impl& impl) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done writing reply notification (#"
                       << sequenceNumber << ")";
          }));
}

void Channel::Impl::readPackets_() {
  auto nopPacketHolder = std::make_shared<NopHolder<Packet>>();
  connection_->read(
      *nopPacketHolder, lazyCallbackWrapper_([nopPacketHolder](Impl& impl) {
        const Packet& nopPacket = nopPacketHolder->getObject();
        if (nopPacket.is<Reply>()) {
          impl.onReply_(*nopPacket.get<Reply>());
        } else if (nopPacket.is<Ack>()) {
          impl.onAck_();
        } else {
          TP_THROW_ASSERT() << "Unexpected packet type: " << nopPacket.index();
        }

        impl.readPackets_();
      }));
}

void Channel::Impl::onReply_(const Reply& nopReply) {
  auto& op = sendOperations_.front();

  TP_VLOG(6) << "Channel " << id_ << " received reply notification (#"
             << op.sequenceNumber << ")";

  const cudaIpcEventHandle_t* stopEvHandle =
      reinterpret_cast<const cudaIpcEventHandle_t*>(
          nopReply.stopEvHandle.c_str());

  op.process(*stopEvHandle);

  TP_VLOG(6) << "Channel " << id_ << " is writing ACK notification (#"
             << op.sequenceNumber << ")";
  auto nopPacketHolder = std::make_shared<NopHolder<Packet>>();
  Packet& nopPacket = nopPacketHolder->getObject();
  nopPacket.Become(nopPacket.index_of<Ack>());

  op.callback(error_);
  sendOperations_.pop_front();

  connection_->write(
      *nopPacketHolder,
      lazyCallbackWrapper_(
          [nopPacketHolder, sequenceNumber{op.sequenceNumber}](Impl& impl) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done writing ACK notification (#" << sequenceNumber
                       << ")";
          }));
}

void Channel::Impl::onAck_() {
  auto& op = recvOperations_.front();

  TP_VLOG(6) << "Channel " << id_ << " received ACK notification (#"
             << op.sequenceNumber << ")";

  recvOperations_.pop_front();
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

  connection_->close();

  for (auto& op : sendOperations_) {
    op.callback(error_);
  }
  sendOperations_.clear();

  // Callbacks for recv operations are always called inline.
  recvOperations_.clear();
}

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
