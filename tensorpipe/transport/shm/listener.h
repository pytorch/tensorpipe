/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/listener.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/socket.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Listener final : public transport::Listener,
                       public std::enable_shared_from_this<Listener>,
                       public EventHandler {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Listener> create(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  using transport::Listener::accept_callback_fn;

  Listener(ConstructorToken, std::shared_ptr<Loop> loop, const Sockaddr& addr);

  ~Listener() override;

  void accept(accept_callback_fn fn) override;

  address_t addr() const override;

  void handleEvents(int events) override;

 private:
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> listener_;
  Sockaddr addr_;
  optional<accept_callback_fn> fn_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
