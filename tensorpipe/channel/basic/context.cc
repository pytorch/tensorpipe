/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/context.h>

#include <algorithm>

#include <tensorpipe/channel/basic/channel.h>
#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {
namespace channel {
namespace basic {

Context::Context()
    : channel::Context("basic"), impl_(std::make_shared<Impl>()) {}

Context::Impl::Impl() : domainDescriptor_("any") {}

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
}

std::shared_ptr<channel::Channel> Context::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint endpoint) {
  return impl_->createChannel(std::move(connection), endpoint);
}

std::shared_ptr<channel::Channel> Context::Impl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint /* unused */) {
  return std::make_shared<Channel>(
      Channel::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(connection));
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  bool wasClosed = false;
  closed_.compare_exchange_strong(wasClosed, true);
  if (!wasClosed) {
    closingEmitter_.close();
  }
}

void Context::join() {
  impl_->join();
}

void Context::Impl::join() {
  close();

  bool wasJoined = false;
  joined_.compare_exchange_strong(wasJoined, true);
  if (!wasJoined) {
    // Nothing to do?
  }
}

Context::~Context() {
  join();
}

} // namespace basic
} // namespace channel
} // namespace tensorpipe
