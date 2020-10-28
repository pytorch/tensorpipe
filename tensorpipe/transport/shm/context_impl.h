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
#include <tuple>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/transport/shm/context.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Context::PrivateIface : public DeferredExecutor {
 public:
  virtual ClosingEmitter& getClosingEmitter() = 0;

  virtual void registerDescriptor(
      int fd,
      int events,
      std::shared_ptr<EpollLoop::EventHandler> h) = 0;

  virtual void unregisterDescriptor(int fd) = 0;

  using TToken = uint32_t;
  using TFunction = std::function<void()>;

  virtual TToken addReaction(TFunction fn) = 0;

  virtual void removeReaction(TToken token) = 0;

  virtual std::tuple<int, int> reactorFds() = 0;

  virtual ~PrivateIface() = default;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
