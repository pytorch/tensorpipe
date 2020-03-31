/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <memory>
#include <mutex>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/reactor.h>
#include <tensorpipe/transport/shm/socket.h>
#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>
#include <tensorpipe/util/ringbuffer/shm.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Connection final : public transport::Connection,
                         public std::enable_shared_from_this<Connection> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Connection> create(
      std::shared_ptr<Loop> loop,
      std::shared_ptr<Socket> socket);

  Connection(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<Socket> socket);

  ~Connection() override;

  // Implementation of transport::Connection.
  void read(read_callback_fn fn) override;

  // Implementation of transport::Connection.
  void read(google::protobuf::MessageLite& message, read_proto_callback_fn fn)
      override;

  // Implementation of transport::Connection.
  void read(void* ptr, size_t length, read_callback_fn fn) override;

  // Implementation of transport::Connection
  void write(const void* ptr, size_t length, write_callback_fn fn) override;

  // Implementation of transport::Connection
  void write(const google::protobuf::MessageLite& message, write_callback_fn fn)
      override;

 private:
  class Impl;

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Impl> impl_;

  // Kickstart connection state machine. Must be called outside
  // constructor because it calls `shared_from_this()`.
  void init_();
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
