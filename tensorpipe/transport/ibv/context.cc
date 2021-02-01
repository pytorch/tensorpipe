/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/context.h>

#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/ibv/connection_impl.h>
#include <tensorpipe/transport/ibv/context_impl.h>
#include <tensorpipe/transport/ibv/listener_impl.h>
#include <tensorpipe/transport/ibv/utility.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

Context::Context() : impl_(ContextImpl::create()) {}

// Explicitly define all methods of the context, which just forward to the impl.
// We cannot use an intermediate ContextBoilerplate class without forcing a
// recursive include of private headers into the public ones.

std::shared_ptr<Connection> Context::connect(std::string addr) {
  return impl_->connect(std::move(addr));
}

std::shared_ptr<Listener> Context::listen(std::string addr) {
  return impl_->listen(std::move(addr));
}

bool Context::isViable() const {
  return impl_->isViable();
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

std::tuple<Error, std::string> Context::lookupAddrForIface(std::string iface) {
  return ibv::lookupAddrForIface(std::move(iface));
}

std::tuple<Error, std::string> Context::lookupAddrForHostname() {
  return ibv::lookupAddrForHostname();
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
