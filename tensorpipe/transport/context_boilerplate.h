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

#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/context_impl_boilerplate.h>

namespace tensorpipe {
namespace transport {

template <typename TCtx, typename TList, typename TConn>
class ContextBoilerplate : public Context {
 public:
  template <typename... Args>
  explicit ContextBoilerplate(Args&&... args);

  ContextBoilerplate(const ContextBoilerplate&) = delete;
  ContextBoilerplate(ContextBoilerplate&&) = delete;
  ContextBoilerplate& operator=(const ContextBoilerplate&) = delete;
  ContextBoilerplate& operator=(ContextBoilerplate&&) = delete;

  std::shared_ptr<Connection> connect(std::string addr) override;

  std::shared_ptr<Listener> listen(std::string addr) override;

  bool isViable() const override;

  const std::string& domainDescriptor() const override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~ContextBoilerplate() override;

 protected:
  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  const std::shared_ptr<TCtx> impl_;
};

template <typename TCtx, typename TList, typename TConn>
template <typename... Args>
ContextBoilerplate<TCtx, TList, TConn>::ContextBoilerplate(Args&&... args)
    : impl_(TCtx::create(std::forward<Args>(args)...)) {
  static_assert(
      std::is_base_of<ContextImplBoilerplate<TCtx, TList, TConn>, TCtx>::value,
      "");
  if (unlikely(!impl_)) {
    return;
  }
  impl_->init();
}

template <typename TCtx, typename TList, typename TConn>
std::shared_ptr<Connection> ContextBoilerplate<TCtx, TList, TConn>::connect(
    std::string addr) {
  if (unlikely(!impl_)) {
    return std::make_shared<ConnectionBoilerplate<TCtx, TList, TConn>>(nullptr);
  }
  return impl_->connect(std::move(addr));
}

template <typename TCtx, typename TList, typename TConn>
std::shared_ptr<Listener> ContextBoilerplate<TCtx, TList, TConn>::listen(
    std::string addr) {
  if (unlikely(!impl_)) {
    return std::make_shared<ListenerBoilerplate<TCtx, TList, TConn>>(nullptr);
  }
  return impl_->listen(std::move(addr));
}

template <typename TCtx, typename TList, typename TConn>
bool ContextBoilerplate<TCtx, TList, TConn>::isViable() const {
  return impl_ != nullptr;
}

template <typename TCtx, typename TList, typename TConn>
const std::string& ContextBoilerplate<TCtx, TList, TConn>::domainDescriptor()
    const {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static std::string empty = "";
    return empty;
  }
  return impl_->domainDescriptor();
}

template <typename TCtx, typename TList, typename TConn>
void ContextBoilerplate<TCtx, TList, TConn>::setId(std::string id) {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->setId(std::move(id));
}

template <typename TCtx, typename TList, typename TConn>
void ContextBoilerplate<TCtx, TList, TConn>::close() {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->close();
}

template <typename TCtx, typename TList, typename TConn>
void ContextBoilerplate<TCtx, TList, TConn>::join() {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->join();
}

template <typename TCtx, typename TList, typename TConn>
ContextBoilerplate<TCtx, TList, TConn>::~ContextBoilerplate() {
  join();
}

} // namespace transport
} // namespace tensorpipe
