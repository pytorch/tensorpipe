/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/xth/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/channel/xth/context_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace xth {

namespace {

struct Descriptor {
  uint64_t ptr;
  NOP_STRUCTURE(Descriptor, ptr);
};

} // namespace

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection)
    : ChannelImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)) {}

void ChannelImpl::initImplFromLoop() {
  context_->enroll(*this);
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CpuBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_VLOG(6) << "Channel " << id_ << " is reading notification (#"
             << sequenceNumber << ")";
  connection_->read(
      nullptr,
      0,
      eagerCallbackWrapper_([sequenceNumber, callback{std::move(callback)}](
                                ChannelImpl& impl,
                                const void* /* unused */,
                                size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done reading notification (#"
                   << sequenceNumber << ")";
        callback(impl.error_);
      }));

  NopHolder<Descriptor> nopHolder;
  Descriptor& nopDescriptor = nopHolder.getObject();
  nopDescriptor.ptr = reinterpret_cast<std::uintptr_t>(buffer.ptr);

  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CpuBuffer buffer,
    TRecvCallback callback) {
  NopHolder<Descriptor> nopHolder;
  loadDescriptor(nopHolder, descriptor);
  Descriptor& nopDescriptor = nopHolder.getObject();
  void* remotePtr = reinterpret_cast<void*>(nopDescriptor.ptr);
  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#" << sequenceNumber
             << ")";
  context_->requestCopy(
      remotePtr,
      buffer.ptr,
      buffer.length,
      eagerCallbackWrapper_([sequenceNumber,
                             callback{std::move(callback)}](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done copying payload (#"
                   << sequenceNumber << ")";
        // Let peer know we've completed the copy.
        TP_VLOG(6) << "Channel " << impl.id_ << " is writing notification (#"
                   << sequenceNumber << ")";
        impl.connection_->write(
            nullptr,
            0,
            impl.lazyCallbackWrapper_([sequenceNumber](ChannelImpl& impl) {
              TP_VLOG(6) << "Channel " << impl.id_
                         << " done writing notification (#" << sequenceNumber
                         << ")";
            }));

        callback(impl.error_);
      }));
}

void ChannelImpl::handleErrorImpl() {
  connection_->close();

  context_->unenroll(*this);
}

} // namespace xth
} // namespace channel
} // namespace tensorpipe
