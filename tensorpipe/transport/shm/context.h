/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Connection;
class EventHandler;
class Listener;

class Context : public transport::Context {
 public:
  Context();

  std::shared_ptr<transport::Connection> connect(std::string addr) override;

  std::shared_ptr<transport::Listener> listen(std::string addr) override;

  const std::string& domainDescriptor() const override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  class PrivateIface {
   public:
    virtual ClosingEmitter& getClosingEmitter() = 0;

    virtual bool inLoopThread() = 0;

    virtual void deferToLoop(std::function<void()> fn) = 0;

    virtual void runInLoop(std::function<void()> fn) = 0;

    virtual void registerDescriptor(
        int fd,
        int events,
        std::shared_ptr<EventHandler> h) = 0;

    virtual void unregisterDescriptor(int fd) = 0;

    using TToken = uint32_t;
    using TFunction = std::function<void()>;

    virtual TToken addReaction(TFunction fn) = 0;

    virtual void removeReaction(TToken token) = 0;

    virtual std::tuple<int, int> reactorFds() = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl;

  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  std::shared_ptr<Impl> impl_;

  // Allow listener to see the private interface.
  friend class Listener;
  // Allow connection to see the private interface.
  friend class Connection;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
