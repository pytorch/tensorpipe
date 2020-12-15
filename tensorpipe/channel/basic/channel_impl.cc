/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/channel_impl.h>

#include <memory>
#include <string>
#include <utility>

#include <tensorpipe/channel/basic/context_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace basic {

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
  TP_VLOG(6) << "Channel " << id_ << " is writing payload (#" << sequenceNumber
             << ")";
  connection_->write(
      buffer.ptr,
      buffer.length,
      eagerCallbackWrapper_(
          [sequenceNumber, callback{std::move(callback)}](ChannelImpl& impl) {
            TP_VLOG(6) << "Channel " << impl.id_ << " done writing payload (#"
                       << sequenceNumber << ")";
            callback(impl.error_);
          }));

  descriptorCallback(Error::kSuccess, std::string());
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CpuBuffer buffer,
    TRecvCallback callback) {
  TP_DCHECK_EQ(descriptor, std::string());

  TP_VLOG(6) << "Channel " << id_ << " is reading payload (#" << sequenceNumber
             << ")";
  connection_->read(
      buffer.ptr,
      buffer.length,
      eagerCallbackWrapper_([sequenceNumber, callback{std::move(callback)}](
                                ChannelImpl& impl,
                                const void* /* unused */,
                                size_t /* unused */) {
        TP_VLOG(6) << "Channel " << impl.id_ << " done reading payload (#"
                   << sequenceNumber << ")";
        callback(impl.error_);
      }));
}

void ChannelImpl::handleErrorImpl() {
  // Close the connection so that all current operations will be aborted. This
  // will cause their callbacks to be invoked, and only then we'll invoke ours.
  connection_->close();

  context_->unenroll(*this);
}

} // namespace basic
} // namespace channel
} // namespace tensorpipe
