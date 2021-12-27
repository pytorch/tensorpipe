/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/listener.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <tensorpipe/core/listener_impl.h>

namespace tensorpipe {

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    const std::vector<std::string>& urls)
    : impl_(std::make_shared<ListenerImpl>(
          std::move(context),
          std::move(id),
          urls)) {
  impl_->init();
}

void Listener::close() {
  impl_->close();
}

Listener::~Listener() {
  close();
}

void Listener::accept(accept_callback_fn fn) {
  impl_->accept(std::move(fn));
}

const std::map<std::string, std::string>& Listener::addresses() const {
  return impl_->addresses();
}

const std::string& Listener::address(const std::string& transport) const {
  return impl_->address(transport);
}

std::string Listener::url(const std::string& transport) const {
  return impl_->url(transport);
}

} // namespace tensorpipe
