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

#include <tensorpipe/transport/listener.h>
#include <tensorpipe/transport/listener_impl_boilerplate.h>

namespace tensorpipe {
namespace transport {

template <typename TCtx, typename TList, typename TConn>
class ListenerBoilerplate : public Listener {
 public:
  template <typename... Args>
  ListenerBoilerplate(
      typename ListenerImplBoilerplate<TCtx, TList, TConn>::ConstructorToken
          token,
      std::shared_ptr<TCtx> context,
      std::string id,
      Args... args);

  explicit ListenerBoilerplate(std::shared_ptr<TList> listener);

  ListenerBoilerplate(const ListenerBoilerplate&) = delete;
  ListenerBoilerplate(ListenerBoilerplate&&) = delete;
  ListenerBoilerplate& operator=(const ListenerBoilerplate&) = delete;
  ListenerBoilerplate& operator=(ListenerBoilerplate&&) = delete;

  // Queue a callback to be called when a connection comes in.
  void accept(accept_callback_fn fn) override;

  // Obtain the listener's address.
  std::string addr() const override;

  // Tell the listener what its identifier is.
  void setId(std::string id) override;

  // Shut down the connection and its resources.
  void close() override;

  ~ListenerBoilerplate() override;

 protected:
  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  const std::shared_ptr<TList> impl_;
};

template <typename TCtx, typename TList, typename TConn>
template <typename... Args>
ListenerBoilerplate<TCtx, TList, TConn>::ListenerBoilerplate(
    typename ListenerImplBoilerplate<TCtx, TList, TConn>::ConstructorToken
        token,
    std::shared_ptr<TCtx> context,
    std::string id,
    Args... args)
    : impl_(std::make_shared<TList>(
          token,
          std::move(context),
          std::move(id),
          std::forward<Args>(args)...)) {
  static_assert(
      std::is_base_of<ListenerImplBoilerplate<TCtx, TList, TConn>, TList>::
          value,
      "");
  impl_->init();
}

template <typename TCtx, typename TList, typename TConn>
ListenerBoilerplate<TCtx, TList, TConn>::ListenerBoilerplate(
    std::shared_ptr<TList> listener)
    : impl_(std::move(listener)) {
  static_assert(
      std::is_base_of<ListenerImplBoilerplate<TCtx, TList, TConn>, TList>::
          value,
      "");
}

template <typename TCtx, typename TList, typename TConn>
void ListenerBoilerplate<TCtx, TList, TConn>::accept(accept_callback_fn fn) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    fn(error, nullptr);
    return;
  }
  impl_->accept(std::move(fn));
}

template <typename TCtx, typename TList, typename TConn>
std::string ListenerBoilerplate<TCtx, TList, TConn>::addr() const {
  if (unlikely(!impl_)) {
    return "";
  }
  return impl_->addr();
}

template <typename TCtx, typename TList, typename TConn>
void ListenerBoilerplate<TCtx, TList, TConn>::setId(std::string id) {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->setId(std::move(id));
}

template <typename TCtx, typename TList, typename TConn>
void ListenerBoilerplate<TCtx, TList, TConn>::close() {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->close();
}

template <typename TCtx, typename TList, typename TConn>
ListenerBoilerplate<TCtx, TList, TConn>::~ListenerBoilerplate() {
  close();
}

} // namespace transport
} // namespace tensorpipe
