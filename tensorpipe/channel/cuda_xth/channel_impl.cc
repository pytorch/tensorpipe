/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_xth/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <cuda_runtime.h>
#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cuda_xth/context_impl.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_xth {

namespace {

class SendOperation {
 public:
  explicit SendOperation(int deviceIdx, CudaBuffer buffer)
      : buffer_(buffer), deviceIdx_(deviceIdx), startEv_(deviceIdx) {
    startEv_.record(buffer_.stream);
  }

  void process(int dstDeviceIdx, CudaBuffer dstBuffer) {
    startEv_.wait(dstBuffer.stream, dstDeviceIdx);

    TP_DCHECK_EQ(buffer_.length, dstBuffer.length);
    TP_CUDA_CHECK(cudaMemcpyAsync(
        dstBuffer.ptr,
        buffer_.ptr,
        dstBuffer.length,
        cudaMemcpyDeviceToDevice,
        dstBuffer.stream));

    CudaEvent stopEv(dstDeviceIdx);
    stopEv.record(dstBuffer.stream);
    stopEv.wait(buffer_.stream, deviceIdx_);
  }

 private:
  const CudaBuffer buffer_;
  const int deviceIdx_;
  CudaEvent startEv_;
};

struct Descriptor {
  uintptr_t opPtr;
  NOP_STRUCTURE(Descriptor, opPtr);
};

} // namespace

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
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);

  // The op must be kept alive until the notification has been received.
  auto op = std::make_shared<SendOperation>(deviceIdx, buffer);
  NopHolder<Descriptor> nopHolder;
  Descriptor& nopDescriptor = nopHolder.getObject();
  nopDescriptor.opPtr = reinterpret_cast<uintptr_t>(op.get());

  TP_VLOG(6) << "Channel " << id_ << " is reading notification (#"
             << sequenceNumber << ")";
  connection_->read(
      nullptr,
      0,
      eagerCallbackWrapper_(
          [sequenceNumber, op{std::move(op)}, callback{std::move(callback)}](
              ChannelImpl& impl,
              const void* /* unused */,
              size_t /* unused */) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done reading notification (#" << sequenceNumber
                       << ")";
            callback(impl.error_);
          }));

  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  int deviceIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);

  NopHolder<Descriptor> nopHolder;
  loadDescriptor(nopHolder, descriptor);
  Descriptor& nopDescriptor = nopHolder.getObject();
  SendOperation* op = reinterpret_cast<SendOperation*>(nopDescriptor.opPtr);

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#" << sequenceNumber
             << ")";
  op->process(deviceIdx, buffer);
  TP_VLOG(6) << "Channel " << id_ << " done copying payload (#"
             << sequenceNumber << ")";

  // Let peer know we've completed the copy.
  TP_VLOG(6) << "Channel " << id_ << " is writing notification (#"
             << sequenceNumber << ")";
  connection_->write(
      nullptr, 0, lazyCallbackWrapper_([sequenceNumber](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done writing notification (#"
                   << sequenceNumber << ")";
      }));

  callback(error_);
}

void ChannelImpl::handleErrorImpl() {
  connection_->close();

  context_->unenroll(*this);
}

} // namespace cuda_xth
} // namespace channel
} // namespace tensorpipe
