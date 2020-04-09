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
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>

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

void Context::close() {
  loop_->close();
}

void Context::join() {
  close();
  loop_->join();
}

Context::~Context() {
  join();
}

std::shared_ptr<transport::Connection> Context::connect(address_t addr) {
  return std::make_shared<Connection>(
      Connection::ConstructorToken(), loop_, std::move(addr));
}

std::shared_ptr<transport::Listener> Context::listen(address_t addr) {
  return std::make_shared<Listener>(
      Listener::ConstructorToken(), loop_, std::move(addr));
}

const std::string& Context::domainDescriptor() const {
  return domainDescriptor_;
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
