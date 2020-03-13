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
#include <mutex>
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
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
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

  void handleEventsFromReactor(int events) override;

 private:
  std::mutex mutex_;
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> listener_;
  Sockaddr addr_;
  std::deque<accept_callback_fn> fns_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
