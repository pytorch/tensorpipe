/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/channel/channel_impl_boilerplate.h>

namespace tensorpipe {
namespace channel {

template <typename TBuffer, typename TCtx, typename TChan>
class ChannelBoilerplate : public Channel<TBuffer> {
 public:
  template <typename... Args>
  ChannelBoilerplate(
      typename ChannelImplBoilerplate<TBuffer, TCtx, TChan>::ConstructorToken
          token,
      std::shared_ptr<TCtx> context,
      std::string id,
      Args&&... args);

  ChannelBoilerplate(const ChannelBoilerplate&) = delete;
  ChannelBoilerplate(ChannelBoilerplate&&) = delete;
  ChannelBoilerplate& operator=(const ChannelBoilerplate&) = delete;
  ChannelBoilerplate& operator=(ChannelBoilerplate&&) = delete;

  // Perform a send operation.
  void send(
      TBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback) override;

  // Queue a recv operation.
  void recv(TDescriptor descriptor, TBuffer buffer, TRecvCallback callback)
      override;

  // Tell the connection what its identifier is.
  void setId(std::string id) override;

  // Shut down the connection and its resources.
  void close() override;

  ~ChannelBoilerplate() override;

 protected:
  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  const std::shared_ptr<TChan> impl_;
};

template <typename TBuffer, typename TCtx, typename TChan>
template <typename... Args>
ChannelBoilerplate<TBuffer, TCtx, TChan>::ChannelBoilerplate(
    typename ChannelImplBoilerplate<TBuffer, TCtx, TChan>::ConstructorToken
        token,
    std::shared_ptr<TCtx> context,
    std::string id,
    Args&&... args)
    : impl_(std::make_shared<TChan>(
          token,
          std::move(context),
          std::move(id),
          std::forward<Args>(args)...)) {
  static_assert(
      std::is_base_of<ChannelImplBoilerplate<TBuffer, TCtx, TChan>, TChan>::
          value,
      "");
  impl_->init();
}

template <typename TBuffer, typename TCtx, typename TChan>
void ChannelBoilerplate<TBuffer, TCtx, TChan>::send(
    TBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  impl_->send(buffer, std::move(descriptorCallback), std::move(callback));
}

template <typename TBuffer, typename TCtx, typename TChan>
void ChannelBoilerplate<TBuffer, TCtx, TChan>::recv(
    TDescriptor descriptor,
    TBuffer buffer,
    TRecvCallback callback) {
  impl_->recv(std::move(descriptor), buffer, std::move(callback));
}

template <typename TBuffer, typename TCtx, typename TChan>
void ChannelBoilerplate<TBuffer, TCtx, TChan>::setId(std::string id) {
  impl_->setId(std::move(id));
}

template <typename TBuffer, typename TCtx, typename TChan>
void ChannelBoilerplate<TBuffer, TCtx, TChan>::close() {
  impl_->close();
}

template <typename TBuffer, typename TCtx, typename TChan>
ChannelBoilerplate<TBuffer, TCtx, TChan>::~ChannelBoilerplate() {
  close();
}

} // namespace channel
} // namespace tensorpipe
