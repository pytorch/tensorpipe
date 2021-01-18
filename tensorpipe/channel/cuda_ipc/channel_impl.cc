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
  std::string handle;
  size_t offset;
  std::string startEvHandle;
  NOP_STRUCTURE(Descriptor, handle, offset, startEvHandle);
};

struct Reply {
  std::string stopEvHandle;
  NOP_STRUCTURE(Reply, stopEvHandle);
};

struct Ack {
  NOP_STRUCTURE(Ack);
};

using Packet = nop::Variant<Reply, Ack>;

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

Descriptor SendOperation::descriptor(const CudaLib& cudaLib) {
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
  return Descriptor{
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
    const cudaIpcMemHandle_t& remoteHandle,
    size_t offset) {
  CudaEvent startEv(deviceIdx_, startEvHandle);
  startEv.wait(stream_, deviceIdx_);

  void* remotePtr;
  TP_CUDA_CHECK(cudaIpcOpenMemHandle(
      &remotePtr, remoteHandle, cudaIpcMemLazyEnablePeerAccess));
  TP_CUDA_CHECK(cudaMemcpyAsync(
      ptr_,
      static_cast<uint8_t*>(remotePtr) + offset,
      length_,
      cudaMemcpyDeviceToDevice,
      stream_));
  TP_CUDA_CHECK(cudaIpcCloseMemHandle(remotePtr));

  stopEv_.record(stream_);
}

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection)
    : ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);

  readPackets();
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
  nopHolder.getObject() = op.descriptor(context_->getCudaLib());
  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);
  // Need to guard otherwise some op on the receiver will crash.
  // TODO: figure out which CUDA op crashed and replace this with a
  // more precise fix.
  CudaDeviceGuard guard(deviceIdx);
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

  op.process(*startEvHandle, *remoteHandle, nopDescriptor.offset);

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
      lazyCallbackWrapper_([nopPacketHolder, sequenceNumber{op.sequenceNumber}](
                               ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing reply notification (#" << sequenceNumber
                   << ")";
      }));
}

void ChannelImpl::readPackets() {
  auto nopPacketHolder = std::make_shared<NopHolder<Packet>>();
  connection_->read(
      *nopPacketHolder,
      lazyCallbackWrapper_([nopPacketHolder](ChannelImpl& impl) {
        const Packet& nopPacket = nopPacketHolder->getObject();
        if (nopPacket.is<Reply>()) {
          impl.onReply(*nopPacket.get<Reply>());
        } else if (nopPacket.is<Ack>()) {
          impl.onAck();
        } else {
          TP_THROW_ASSERT() << "Unexpected packet type: " << nopPacket.index();
        }

        impl.readPackets();
      }));
}

void ChannelImpl::onReply(const Reply& nopReply) {
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

  connection_->write(
      *nopPacketHolder,
      lazyCallbackWrapper_([nopPacketHolder, sequenceNumber{op.sequenceNumber}](
                               ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing ACK notification (#" << sequenceNumber
                   << ")";
      }));

  sendOperations_.pop_front();
}

void ChannelImpl::onAck() {
  auto& op = recvOperations_.front();

  TP_VLOG(6) << "Channel " << id_ << " received ACK notification (#"
             << op.sequenceNumber << ")";

  recvOperations_.pop_front();
}

void ChannelImpl::handleErrorImpl() {
  connection_->close();

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
