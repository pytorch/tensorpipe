/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cma/context_impl.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cma {

namespace {

struct Descriptor {
  uint32_t pid;
  uint64_t ptr;
  NOP_STRUCTURE(Descriptor, pid, ptr);
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
  nopDescriptor.pid = getpid();
  nopDescriptor.ptr = reinterpret_cast<uint64_t>(buffer.ptr);

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
  pid_t remotePid = nopDescriptor.pid;
  void* remotePtr = reinterpret_cast<void*>(nopDescriptor.ptr);

  TP_VLOG(6) << "Channel " << id_ << " is copying payload (#" << sequenceNumber
             << ")";
  context_->requestCopy(
      remotePid,
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

} // namespace cma
} // namespace channel
} // namespace tensorpipe
