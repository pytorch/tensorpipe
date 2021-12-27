/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/context_impl.h>

#include <tensorpipe/transport/uv/connection_impl.h>
#include <tensorpipe/transport/uv/listener_impl.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

namespace {

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"uv:"};

std::string generateDomainDescriptor() {
  return kDomainDescriptorPrefix + "*";
}

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create() {
  return std::make_shared<ContextImpl>();
}

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          generateDomainDescriptor()) {}

void ContextImpl::handleErrorImpl() {
  loop_.close();
}

void ContextImpl::joinImpl() {
  loop_.join();
}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

std::unique_ptr<TCPHandle> ContextImpl::createHandle() {
  return std::make_unique<TCPHandle>(loop_.ptr(), loop_);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
