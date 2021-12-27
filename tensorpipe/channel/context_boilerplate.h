/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/channel/context_impl_boilerplate.h>

namespace tensorpipe {
namespace channel {

template <typename TCtx, typename TChan>
class ContextBoilerplate : public Context {
 public:
  template <typename... Args>
  explicit ContextBoilerplate(Args&&... args);

  ContextBoilerplate(const ContextBoilerplate&) = delete;
  ContextBoilerplate(ContextBoilerplate&&) = delete;
  ContextBoilerplate& operator=(const ContextBoilerplate&) = delete;
  ContextBoilerplate& operator=(ContextBoilerplate&&) = delete;

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint) override;

  size_t numConnectionsNeeded() const override;

  bool isViable() const override;

  const std::unordered_map<Device, std::string>& deviceDescriptors()
      const override;

  bool canCommunicateWithRemote(
      const std::string& localDeviceDescriptor,
      const std::string& remoteDeviceDescriptor) const override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~ContextBoilerplate() override;

 protected:
  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it. However, its lifetime is tied to the one
  // of this public object since when the latter is destroyed the implementation
  // is closed and joined.
  const std::shared_ptr<TCtx> impl_;
};

template <typename TCtx, typename TChan>
template <typename... Args>
ContextBoilerplate<TCtx, TChan>::ContextBoilerplate(Args&&... args)
    : impl_(TCtx::create(std::forward<Args>(args)...)) {
  static_assert(
      std::is_base_of<ChannelImplBoilerplate<TCtx, TChan>, TChan>::value, "");
  if (unlikely(!impl_)) {
    return;
  }
  impl_->init();
}

template <typename TCtx, typename TChan>
std::shared_ptr<Channel> ContextBoilerplate<TCtx, TChan>::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint endpoint) {
  if (unlikely(!impl_)) {
    return std::make_shared<ChannelBoilerplate<TCtx, TChan>>(nullptr);
  }
  return impl_->createChannel(std::move(connections), endpoint);
}

template <typename TCtx, typename TChan>
size_t ContextBoilerplate<TCtx, TChan>::numConnectionsNeeded() const {
  if (unlikely(!impl_)) {
    return 0;
  }
  return impl_->numConnectionsNeeded();
}

template <typename TCtx, typename TChan>
bool ContextBoilerplate<TCtx, TChan>::isViable() const {
  return impl_ != nullptr;
}

template <typename TCtx, typename TChan>
const std::unordered_map<Device, std::string>& ContextBoilerplate<TCtx, TChan>::
    deviceDescriptors() const {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static std::unordered_map<Device, std::string> empty = {};
    return empty;
  }
  return impl_->deviceDescriptors();
}

template <typename TCtx, typename TChan>
bool ContextBoilerplate<TCtx, TChan>::canCommunicateWithRemote(
    const std::string& localDeviceDescriptor,
    const std::string& remoteDeviceDescriptor) const {
  if (unlikely(!impl_)) {
    return false;
  }
  return impl_->canCommunicateWithRemote(
      localDeviceDescriptor, remoteDeviceDescriptor);
}

template <typename TCtx, typename TChan>
void ContextBoilerplate<TCtx, TChan>::setId(std::string id) {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->setId(std::move(id));
}

template <typename TCtx, typename TChan>
void ContextBoilerplate<TCtx, TChan>::close() {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->close();
}

template <typename TCtx, typename TChan>
void ContextBoilerplate<TCtx, TChan>::join() {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->join();
}

template <typename TCtx, typename TChan>
ContextBoilerplate<TCtx, TChan>::~ContextBoilerplate() {
  join();
}

} // namespace channel
} // namespace tensorpipe
