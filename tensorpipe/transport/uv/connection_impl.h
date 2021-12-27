/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <deque>
#include <memory>
#include <string>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/stream_read_write_ops.h>
#include <tensorpipe/transport/connection_impl_boilerplate.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class ContextImpl;
class ListenerImpl;

class ConnectionImpl final : public ConnectionImplBoilerplate<
                                 ContextImpl,
                                 ListenerImpl,
                                 ConnectionImpl> {
 public:
  // Create a connection that is already connected (e.g. from a listener).
  ConnectionImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::unique_ptr<TCPHandle> handle);

  // Create a connection that connects to the specified address.
  ConnectionImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::string addr);

 protected:
  // Implement the entry points called by ConnectionImplBoilerplate.
  void initImplFromLoop() override;
  void readImplFromLoop(read_callback_fn fn) override;
  void readImplFromLoop(void* ptr, size_t length, read_callback_fn fn) override;
  void writeImplFromLoop(const void* ptr, size_t length, write_callback_fn fn)
      override;
  void handleErrorImpl() override;

 private:
  // Called when libuv is about to read data from connection.
  void allocCallbackFromLoop(uv_buf_t* buf);

  // Called when libuv has read data from connection.
  void readCallbackFromLoop(ssize_t nread, const uv_buf_t* buf);

  // Called when libuv has written data to connection.
  void writeCallbackFromLoop(int status);

  // Called when libuv has closed the handle.
  void closeCallbackFromLoop();

  const std::unique_ptr<TCPHandle> handle_;
  optional<Sockaddr> sockaddr_;

  std::deque<StreamReadOperation> readOperations_;
  std::deque<StreamWriteOperation> writeOperations_;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
