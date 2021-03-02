/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <tensorpipe/channel/cuda_ipc/context_impl.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_ipc {

struct Descriptor {
  std::string allocationId;
  std::string handle;
  size_t offset;
  std::string startEvHandle;
  NOP_STRUCTURE(Descriptor, allocationId, handle, offset, startEvHandle);
};

struct Reply {
  std::string stopEvHandle;
  NOP_STRUCTURE(Reply, stopEvHandle);
};

struct Ack {
  NOP_STRUCTURE(Ack);
};

SendOperation::SendOperation(
    uint64_t sequenceNumber,
    TSendCallback callback,
    int deviceIdx,
    const void* ptr,
    cudaStream_t stream)
    : sequenceNumber(sequenceNumber),
      callback(std::move(callback)),
      deviceIdx_(deviceIdx),
      ptr_(ptr),
      stream_(stream),
      startEv_(deviceIdx_, /* interprocess = */ true) {
  startEv_.record(stream_);
}

Descriptor SendOperation::descriptor(
    const CudaLib& cudaLib,
    const std::string& processIdentifier) {
  CudaDeviceGuard guard(deviceIdx_);
  cudaIpcMemHandle_t handle;
  TP_CUDA_CHECK(cudaIpcGetMemHandle(&handle, const_cast<void*>(ptr_)));
  CUdeviceptr basePtr;
  TP_CUDA_DRIVER_CHECK(
      cudaLib,
      cudaLib.memGetAddressRange(
          &basePtr, nullptr, reinterpret_cast<CUdeviceptr>(ptr_)));
  size_t offset = reinterpret_cast<const uint8_t*>(ptr_) -
      reinterpret_cast<uint8_t*>(basePtr);

  unsigned long long bufferId;
  TP_CUDA_DRIVER_CHECK(
      cudaLib,
      cudaLib.pointerGetAttribute(
          &bufferId, CU_POINTER_ATTRIBUTE_BUFFER_ID, basePtr));

  return Descriptor{
      // FIXME The process identifier will be the same each time, hence we could
      // just send it once during the setup of the channel and omit it later.
      processIdentifier + "_" + std::to_string(bufferId),
      std::string(reinterpret_cast<const char*>(&handle), sizeof(handle)),
      offset,
      startEv_.serializedHandle()};
}

void SendOperation::process(const cudaIpcEventHandle_t& stopEvHandle) {
  CudaEvent stopEv(deviceIdx_, stopEvHandle);
  stopEv.wait(stream_, deviceIdx_);
}

RecvOperation::RecvOperation(
    uint64_t sequenceNumber,
    int deviceIdx,
    void* ptr,
    cudaStream_t stream,
    size_t length)
    : sequenceNumber(sequenceNumber),
      deviceIdx_(deviceIdx),
      ptr_(ptr),
      stream_(stream),
      length_(length),
      stopEv_(deviceIdx_, /* interprocess = */ true) {}

Reply RecvOperation::reply() {
  return Reply{stopEv_.serializedHandle()};
}

void RecvOperation::process(
    const cudaIpcEventHandle_t& startEvHandle,
    void* remotePtr,
    size_t offset) {
  CudaEvent startEv(deviceIdx_, startEvHandle);
  startEv.wait(stream_, deviceIdx_);

  {
    CudaDeviceGuard guard(deviceIdx_);
    TP_CUDA_CHECK(cudaMemcpyAsync(
        ptr_,
        static_cast<uint8_t*>(remotePtr) + offset,
        length_,
        cudaMemcpyDeviceToDevice,
        stream_));
  }

  stopEv_.record(stream_);
}

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> replyConnection,
    std::shared_ptr<transport::Connection> ackConnection)
    : ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      replyConnection_(std::move(replyConnection)),
      ackConnection_(std::move(ackConnection)) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);
  sendOperations_.emplace_back(
      sequenceNumber,
      std::move(callback),
      deviceIdx,
      buffer.ptr,
      buffer.stream);
  auto& op = sendOperations_.back();

  NopHolder<Descriptor> nopHolder;
  nopHolder.getObject() =
      op.descriptor(context_->getCudaLib(), context_->getProcessIdentifier());
  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));

  auto nopReplyHolder = std::make_shared<NopHolder<Reply>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (reply #"
             << sequenceNumber << ")";
  replyConnection_->read(
      *nopReplyHolder,
      callbackWrapper_([&op, nopReplyHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (reply #" << op.sequenceNumber
                   << ")";
        if (!impl.error_) {
          impl.onReply(op, nopReplyHolder->getObject());
        }
      }));
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);
  recvOperations_.emplace_back(
      sequenceNumber, deviceIdx, buffer.ptr, buffer.stream, buffer.length);
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

  void* remoteBasePtr = context_->openIpcHandle(
      nopDescriptor.allocationId, *remoteHandle, deviceIdx);
  op.process(*startEvHandle, remoteBasePtr, nopDescriptor.offset);

  TP_VLOG(6) << "Channel " << id_ << " done copying payload (#"
             << op.sequenceNumber << ")";

  callback(error_);

  // Let peer know we've completed the copy.
  TP_VLOG(6) << "Channel " << id_ << " is writing reply notification (#"
             << op.sequenceNumber << ")";
  auto nopReplyHolder = std::make_shared<NopHolder<Reply>>();
  nopReplyHolder->getObject() = op.reply();

  replyConnection_->write(
      *nopReplyHolder,
      callbackWrapper_([nopReplyHolder,
                        sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing reply notification (#" << sequenceNumber
                   << ")";
      }));

  TP_VLOG(6) << "Channel " << id_ << " is reading ACK notification (#"
             << op.sequenceNumber << ")";
  auto nopAckHolder = std::make_shared<NopHolder<Ack>>();
  ackConnection_->read(
      *nopAckHolder, callbackWrapper_([&op, nopAckHolder](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading ACK notification (#" << op.sequenceNumber
                   << ")";
        if (!impl.error_) {
          impl.onAck(op);
        }
      }));
}

void ChannelImpl::onReply(SendOperation& op, const Reply& nopReply) {
  TP_VLOG(6) << "Channel " << id_ << " received reply notification (#"
             << op.sequenceNumber << ")";

  const cudaIpcEventHandle_t* stopEvHandle =
      reinterpret_cast<const cudaIpcEventHandle_t*>(
          nopReply.stopEvHandle.c_str());

  op.process(*stopEvHandle);

  TP_VLOG(6) << "Channel " << id_ << " is writing ACK notification (#"
             << op.sequenceNumber << ")";
  auto nopAckHolder = std::make_shared<NopHolder<Ack>>();

  op.callback(error_);

  ackConnection_->write(
      *nopAckHolder,
      callbackWrapper_(
          [nopAckHolder, sequenceNumber{op.sequenceNumber}](ChannelImpl& impl) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done writing ACK notification (#" << sequenceNumber
                       << ")";
          }));

  sendOperations_.pop_front();
}

void ChannelImpl::onAck(RecvOperation& op) {
  TP_VLOG(6) << "Channel " << id_ << " received ACK notification (#"
             << op.sequenceNumber << ")";

  recvOperations_.pop_front();
}

void ChannelImpl::handleErrorImpl() {
  replyConnection_->close();
  ackConnection_->close();

  for (auto& op : sendOperations_) {
    op.callback(error_);
  }
  sendOperations_.clear();

  // Callbacks for recv operations are always called inline.
  recvOperations_.clear();

  context_->unenroll(*this);
}

} // namespace cuda_ipc
} // namespace channel
} // namespace tensorpipe
