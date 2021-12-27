/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/context.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <tensorpipe/core/context_impl.h>

namespace tensorpipe {

Context::Context(ContextOptions opts)
    : impl_(std::make_shared<ContextImpl>(std::move(opts))) {
  impl_->init();
}

void Context::registerTransport(
    int64_t priority,
    std::string transport,
    std::shared_ptr<transport::Context> context) {
  impl_->registerTransport(priority, std::move(transport), std::move(context));
}

void Context::registerChannel(
    int64_t priority,
    std::string channel,
    std::shared_ptr<channel::Context> context) {
  impl_->registerChannel(priority, std::move(channel), std::move(context));
}

std::shared_ptr<Listener> Context::listen(
    const std::vector<std::string>& urls) {
  return impl_->listen(urls);
}

std::shared_ptr<Pipe> Context::connect(
    const std::string& url,
    PipeOptions opts) {
  return impl_->connect(url, std::move(opts));
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

} // namespace tensorpipe
