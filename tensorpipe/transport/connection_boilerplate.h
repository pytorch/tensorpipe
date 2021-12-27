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

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/connection_impl_boilerplate.h>

namespace tensorpipe {
namespace transport {

template <typename TCtx, typename TList, typename TConn>
class ConnectionBoilerplate : public Connection {
 public:
  template <typename... Args>
  ConnectionBoilerplate(
      typename ConnectionImplBoilerplate<TCtx, TList, TConn>::ConstructorToken
          token,
      std::shared_ptr<TCtx> context,
      std::string id,
      Args... args);

  explicit ConnectionBoilerplate(std::shared_ptr<TConn> connection);

  ConnectionBoilerplate(const ConnectionBoilerplate&) = delete;
  ConnectionBoilerplate(ConnectionBoilerplate&&) = delete;
  ConnectionBoilerplate& operator=(const ConnectionBoilerplate&) = delete;
  ConnectionBoilerplate& operator=(ConnectionBoilerplate&&) = delete;

  // Queue a read operation.
  void read(read_callback_fn fn) override;
  void read(AbstractNopHolder& object, read_nop_callback_fn fn) override;
  void read(void* ptr, size_t length, read_callback_fn fn) override;

  // Perform a write operation.
  void write(const void* ptr, size_t length, write_callback_fn fn) override;
  void write(const AbstractNopHolder& object, write_callback_fn fn) override;

  // Tell the connection what its identifier is.
  void setId(std::string id) override;

  // Shut down the connection and its resources.
  void close() override;

  ~ConnectionBoilerplate() override;

 protected:
  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  const std::shared_ptr<TConn> impl_;
};

template <typename TCtx, typename TList, typename TConn>
template <typename... Args>
ConnectionBoilerplate<TCtx, TList, TConn>::ConnectionBoilerplate(
    typename ConnectionImplBoilerplate<TCtx, TList, TConn>::ConstructorToken
        token,
    std::shared_ptr<TCtx> context,
    std::string id,
    Args... args)
    : impl_(std::make_shared<TConn>(
          token,
          std::move(context),
          std::move(id),
          std::forward<Args>(args)...)) {
  static_assert(
      std::is_base_of<ConnectionImplBoilerplate<TCtx, TList, TConn>, TConn>::
          value,
      "");
  impl_->init();
}

template <typename TCtx, typename TList, typename TConn>
ConnectionBoilerplate<TCtx, TList, TConn>::ConnectionBoilerplate(
    std::shared_ptr<TConn> connection)
    : impl_(std::move(connection)) {
  static_assert(
      std::is_base_of<ConnectionImplBoilerplate<TCtx, TList, TConn>, TConn>::
          value,
      "");
}

template <typename TCtx, typename TList, typename TConn>
void ConnectionBoilerplate<TCtx, TList, TConn>::read(read_callback_fn fn) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    fn(error, nullptr, 0);
    return;
  }
  impl_->read(std::move(fn));
}

template <typename TCtx, typename TList, typename TConn>
void ConnectionBoilerplate<TCtx, TList, TConn>::read(
    AbstractNopHolder& object,
    read_nop_callback_fn fn) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    fn(error);
    return;
  }
  impl_->read(object, std::move(fn));
}

template <typename TCtx, typename TList, typename TConn>
void ConnectionBoilerplate<TCtx, TList, TConn>::read(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    fn(error, ptr, length);
    return;
  }
  impl_->read(ptr, length, std::move(fn));
}

template <typename TCtx, typename TList, typename TConn>
void ConnectionBoilerplate<TCtx, TList, TConn>::write(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    fn(error);
    return;
  }
  impl_->write(ptr, length, std::move(fn));
}

template <typename TCtx, typename TList, typename TConn>
void ConnectionBoilerplate<TCtx, TList, TConn>::write(
    const AbstractNopHolder& object,
    write_callback_fn fn) {
  if (unlikely(!impl_)) {
    // FIXME In C++-17 perhaps a global static inline variable would be better?
    static Error error = TP_CREATE_ERROR(ContextNotViableError);
    fn(error);
    return;
  }
  impl_->write(object, std::move(fn));
}

template <typename TCtx, typename TList, typename TConn>
void ConnectionBoilerplate<TCtx, TList, TConn>::setId(std::string id) {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->setId(std::move(id));
}

template <typename TCtx, typename TList, typename TConn>
void ConnectionBoilerplate<TCtx, TList, TConn>::close() {
  if (unlikely(!impl_)) {
    return;
  }
  impl_->close();
}

template <typename TCtx, typename TList, typename TConn>
ConnectionBoilerplate<TCtx, TList, TConn>::~ConnectionBoilerplate() {
  close();
}

} // namespace transport
} // namespace tensorpipe
