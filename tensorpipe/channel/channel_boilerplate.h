/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

template <typename TCtx, typename TChan>
class ChannelBoilerplate : public Channel {
 public:
  template <typename... Args>
  ChannelBoilerplate(
      typename ChannelImplBoilerplate<TCtx, TChan>::ConstructorToken token,
      std::shared_ptr<TCtx> context,
      std::string id,
      Args&&... args);

  explicit ChannelBoilerplate(std::shared_ptr<TChan> channel);

  ChannelBoilerplate(const ChannelBoilerplate&) = delete;
  ChannelBoilerplate(ChannelBoilerplate&&) = delete;
  ChannelBoilerplate& operator=(const ChannelBoilerplate&) = delete;
  ChannelBoilerplate& operator=(ChannelBoilerplate&&) = delete;

  // Perform a send operation.
  void send(Buffer buffer, size_t length, TSendCallback callback) override;

  // Queue a recv operation.
  void recv(Buffer buffer, size_t length, TRecvCallback callback) override;

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

template <typename TCtx, typename TChan>
template <typename... Args>
ChannelBoilerplate<TCtx, TChan>::ChannelBoilerplate(
    typename ChannelImplBoilerplate<TCtx, TChan>::ConstructorToken token,
    std::shared_ptr<TCtx> context,
    std::string id,
    Args&&... args)
    : impl_(std::make_shared<TChan>(
          token,
          std::move(context),
          std::move(id),
          std::forward<Args>(args)...)) {
  static_assert(
      std::is_base_of<ChannelImplBoilerplate<TCtx, TChan>, TChan>::value, "");
  impl_->init();
}

template <typename TCtx, typename TChan>
ChannelBoilerplate<TCtx, TChan>::ChannelBoilerplate(
    std::shared_ptr<TChan> channel)
    : impl_(std::move(channel)) {
  static_assert(
      std::is_base_of<ChannelImplBoilerplate<TCtx, TChan>, TChan>::value, "");
}

template <typename TCtx, typename TChan>
void ChannelBoilerplate<TCtx, TChan>::send(
    Buffer buffer,
    size_t length,
    TSendCallback callback) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    callback(error);
    return;
  }
  impl_->send(buffer, length, std::move(callback));
}

template <typename TCtx, typename TChan>
void ChannelBoilerplate<TCtx, TChan>::recv(
    Buffer buffer,
    size_t length,
    TRecvCallback callback) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    callback(error);
    return;
  }
  impl_->recv(buffer, length, std::move(callback));
}

template <typename TCtx, typename TChan>
void ChannelBoilerplate<TCtx, TChan>::setId(std::string id) {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->setId(std::move(id));
}

template <typename TCtx, typename TChan>
void ChannelBoilerplate<TCtx, TChan>::close() {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->close();
}

template <typename TCtx, typename TChan>
ChannelBoilerplate<TCtx, TChan>::~ChannelBoilerplate() {
  close();
}

} // namespace channel
} // namespace tensorpipe
