/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

template <typename TBuffer, typename TCtx, typename TChan>
class ContextBoilerplate : public Context<TBuffer> {
 public:
  template <typename... Args>
  explicit ContextBoilerplate(Args&&... args);

  ContextBoilerplate(const ContextBoilerplate&) = delete;
  ContextBoilerplate(ContextBoilerplate&&) = delete;
  ContextBoilerplate& operator=(const ContextBoilerplate&) = delete;
  ContextBoilerplate& operator=(ContextBoilerplate&&) = delete;

  std::shared_ptr<Channel<TBuffer>> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint) override;

  size_t numConnectionsNeeded() const override;

  bool isViable() const override;

  const std::string& domainDescriptor() const override;

  bool canCommunicateWithRemote(
      const std::string& remoteDomainDescriptor) const override;

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

template <typename TBuffer, typename TCtx, typename TChan>
template <typename... Args>
ContextBoilerplate<TBuffer, TCtx, TChan>::ContextBoilerplate(Args&&... args)
    : impl_(TCtx::create(std::forward<Args>(args)...)) {
  static_assert(
      std::is_base_of<ChannelImplBoilerplate<TBuffer, TCtx, TChan>, TChan>::
          value,
      "");
  impl_->init();
}

template <typename TBuffer, typename TCtx, typename TChan>
std::shared_ptr<Channel<TBuffer>> ContextBoilerplate<TBuffer, TCtx, TChan>::
    createChannel(
        std::vector<std::shared_ptr<transport::Connection>> connections,
        Endpoint endpoint) {
  return impl_->createChannel(std::move(connections), endpoint);
}

template <typename TBuffer, typename TCtx, typename TChan>
size_t ContextBoilerplate<TBuffer, TCtx, TChan>::numConnectionsNeeded() const {
  return impl_->numConnectionsNeeded();
}

template <typename TBuffer, typename TCtx, typename TChan>
bool ContextBoilerplate<TBuffer, TCtx, TChan>::isViable() const {
  return impl_->isViable();
}

template <typename TBuffer, typename TCtx, typename TChan>
const std::string& ContextBoilerplate<TBuffer, TCtx, TChan>::domainDescriptor()
    const {
  return impl_->domainDescriptor();
}

template <typename TBuffer, typename TCtx, typename TChan>
bool ContextBoilerplate<TBuffer, TCtx, TChan>::canCommunicateWithRemote(
    const std::string& remoteDomainDescriptor) const {
  return impl_->canCommunicateWithRemote(remoteDomainDescriptor);
}

template <typename TBuffer, typename TCtx, typename TChan>
void ContextBoilerplate<TBuffer, TCtx, TChan>::setId(std::string id) {
  impl_->setId(std::move(id));
}

template <typename TBuffer, typename TCtx, typename TChan>
void ContextBoilerplate<TBuffer, TCtx, TChan>::close() {
  impl_->close();
}

template <typename TBuffer, typename TCtx, typename TChan>
void ContextBoilerplate<TBuffer, TCtx, TChan>::join() {
  impl_->join();
}

template <typename TBuffer, typename TCtx, typename TChan>
ContextBoilerplate<TBuffer, TCtx, TChan>::~ContextBoilerplate() {
  join();
}

} // namespace channel
} // namespace tensorpipe
