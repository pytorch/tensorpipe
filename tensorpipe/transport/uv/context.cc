/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/context.h>

#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/listener.h>

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

Context::Context()
    : loop_(Loop::create()), domainDescriptor_(generateDomainDescriptor()) {}

Context::~Context() {}

void Context::join() {
  loop_->join();
}

std::shared_ptr<transport::Connection> Context::connect(address_t addr) {
  return Connection::create(loop_, Sockaddr::createInetSockAddr(addr));
}

std::shared_ptr<transport::Listener> Context::listen(address_t addr) {
  return Listener::create(loop_, Sockaddr::createInetSockAddr(addr));
}

const std::string& Context::domainDescriptor() const {
  return domainDescriptor_;
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
