/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/channel.h>

namespace tensorpipe {
namespace channel {

Channel::~Channel() {}

std::pair<Channel::TDescriptor, std::future<void>> Channel::send(
    const void* ptr,
    size_t length) {
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  auto descriptor = send(
      ptr, length, [promise{std::move(promise)}] { promise->set_value(); });
  return {std::move(descriptor), std::move(future)};
}

std::future<void> Channel::recv(
    TDescriptor descriptor,
    void* ptr,
    size_t length) {
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  recv(std::move(descriptor), ptr, length, [promise{std::move(promise)}] {
    promise->set_value();
  });
  return future;
}

ChannelFactory::ChannelFactory(std::string name) : name_(std::move(name)) {}

ChannelFactory::~ChannelFactory() {}

const std::string& ChannelFactory::name() const {
  return name_;
}

} // namespace channel
} // namespace tensorpipe
