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

#include <tensorpipe/transport/listener.h>
#include <tensorpipe/transport/listener_impl_boilerplate.h>

namespace tensorpipe {
namespace transport {

template <typename TImpl, typename TContextImpl>
class ListenerBoilerplate : public Listener {
 public:
  ListenerBoilerplate(std::shared_ptr<TImpl> impl);

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
  const std::shared_ptr<TImpl> impl_;
};

template <typename TImpl, typename TContextImpl>
ListenerBoilerplate<TImpl, TContextImpl>::ListenerBoilerplate(
    std::shared_ptr<TImpl> impl)
    : impl_(std::move(impl)) {
  static_assert(
      std::is_base_of<ListenerImplBoilerplate<TImpl, TContextImpl>, TImpl>::
          value,
      "");
  impl_->init();
}

template <typename TImpl, typename TContextImpl>
void ListenerBoilerplate<TImpl, TContextImpl>::accept(accept_callback_fn fn) {
  impl_->accept(std::move(fn));
}

template <typename TImpl, typename TContextImpl>
std::string ListenerBoilerplate<TImpl, TContextImpl>::addr() const {
  return impl_->addr();
}

template <typename TImpl, typename TContextImpl>
void ListenerBoilerplate<TImpl, TContextImpl>::setId(std::string id) {
  impl_->setId(std::move(id));
}

template <typename TImpl, typename TContextImpl>
void ListenerBoilerplate<TImpl, TContextImpl>::close() {
  impl_->close();
}

template <typename TImpl, typename TContextImpl>
ListenerBoilerplate<TImpl, TContextImpl>::~ListenerBoilerplate() {
  close();
}

} // namespace transport
} // namespace tensorpipe