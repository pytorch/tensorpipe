/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/mpt/context.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <tensorpipe/channel/mpt/channel_impl.h>
#include <tensorpipe/channel/mpt/context_impl.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

Context::Context(
    std::vector<std::shared_ptr<transport::Context>> contexts,
    std::vector<std::shared_ptr<transport::Listener>> listeners)
    : impl_(std::make_shared<ContextImpl>(
          std::move(contexts),
          std::move(listeners))) {
  impl_->init();
}

// Explicitly define all methods of the context, which just forward to the impl.
// We cannot use an intermediate ContextBoilerplate class without forcing a
// recursive include of private headers into the public ones.

std::shared_ptr<CpuChannel> Context::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint endpoint) {
  return impl_->createChannel(std::move(connection), endpoint);
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

void Context::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Context::close() {
  impl_->close();
}

void Context::join() {
  impl_->join();
}

Context::~Context() {
  join();
}

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
