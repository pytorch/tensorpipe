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

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/connection_impl_boilerplate.h>

namespace tensorpipe {
namespace transport {

template <typename TImpl, typename TContextImpl>
class ConnectionBoilerplate : public Connection {
 public:
  ConnectionBoilerplate(std::shared_ptr<TImpl> impl);

  // Queue a read operation.
  void read(read_callback_fn fn) override;
  void read(void* ptr, size_t length, read_callback_fn fn) override;

  // Perform a write operation.
  void write(const void* ptr, size_t length, write_callback_fn fn) override;

  // Tell the connection what its identifier is.
  void setId(std::string id) override;

  // Shut down the connection and its resources.
  void close() override;

  ~ConnectionBoilerplate() override;

 protected:
  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  const std::shared_ptr<TImpl> impl_;
};

template <typename TImpl, typename TContextImpl>
ConnectionBoilerplate<TImpl, TContextImpl>::ConnectionBoilerplate(
    std::shared_ptr<TImpl> impl)
    : impl_(std::move(impl)) {
  static_assert(
      std::is_base_of<ConnectionImplBoilerplate<TImpl, TContextImpl>, TImpl>::
          value,
      "");
  impl_->init();
}

template <typename TImpl, typename TContextImpl>
void ConnectionBoilerplate<TImpl, TContextImpl>::read(read_callback_fn fn) {
  impl_->read(std::move(fn));
}

template <typename TImpl, typename TContextImpl>
void ConnectionBoilerplate<TImpl, TContextImpl>::read(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  impl_->read(ptr, length, std::move(fn));
}

template <typename TImpl, typename TContextImpl>
void ConnectionBoilerplate<TImpl, TContextImpl>::write(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  impl_->write(ptr, length, std::move(fn));
}

template <typename TImpl, typename TContextImpl>
void ConnectionBoilerplate<TImpl, TContextImpl>::setId(std::string id) {
  impl_->setId(std::move(id));
}

template <typename TImpl, typename TContextImpl>
void ConnectionBoilerplate<TImpl, TContextImpl>::close() {
  impl_->close();
}

template <typename TImpl, typename TContextImpl>
ConnectionBoilerplate<TImpl, TContextImpl>::~ConnectionBoilerplate() {
  close();
}

} // namespace transport
} // namespace tensorpipe
