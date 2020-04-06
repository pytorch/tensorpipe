/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class Listener;
class Pipe;

class Context final : public std::enable_shared_from_this<Context> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Context> create();

  explicit Context(ConstructorToken);

  void registerTransport(
      int64_t,
      std::string,
      std::shared_ptr<transport::Context>);

  void registerChannelFactory(
      int64_t,
      std::string,
      std::shared_ptr<channel::ChannelFactory>);

  std::shared_ptr<Listener> listen(const std::vector<std::string>&);

  std::shared_ptr<Pipe> connect(const std::string&);

  // Put the context in a terminal state, in turn closing all of its pipes and
  // listeners, and release its resources. This may be done asynchronously, in
  // background.
  void close();

  // Wait for all resources to be released and all background activity to stop.
  void join();

  ~Context();

 private:
  class PrivateIface {
   public:
    virtual ClosingEmitter& getClosingEmitter() = 0;

    virtual std::shared_ptr<transport::Context> getContextForTransport(
        const std::string&) = 0;

    virtual std::shared_ptr<channel::ChannelFactory> getChannelFactory(
        const std::string&) = 0;

    using TOrderedContexts = std::map<
        int64_t,
        std::tuple<std::string, std::shared_ptr<transport::Context>>>;

    virtual const TOrderedContexts& getOrderedContexts() = 0;

    using TOrderedChannelFactories = std::map<
        int64_t,
        std::tuple<std::string, std::shared_ptr<channel::ChannelFactory>>>;

    virtual const TOrderedChannelFactories& getOrderedChannelFactories() = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl : public PrivateIface, public std::enable_shared_from_this<Impl> {
    // Use the passkey idiom to allow make_shared to call what should be a
    // private constructor. See https://abseil.io/tips/134 for more information.
    struct ConstructorToken {};

   public:
    static std::shared_ptr<Impl> create();

    Impl(ConstructorToken);

    void registerTransport(
        int64_t,
        std::string,
        std::shared_ptr<transport::Context>);

    void registerChannelFactory(
        int64_t,
        std::string,
        std::shared_ptr<channel::ChannelFactory>);

    std::shared_ptr<Listener> listen(const std::vector<std::string>&);

    std::shared_ptr<Pipe> connect(const std::string&);

    ClosingEmitter& getClosingEmitter() override;

    std::shared_ptr<transport::Context> getContextForTransport(
        const std::string&) override;
    std::shared_ptr<channel::ChannelFactory> getChannelFactory(
        const std::string&) override;

    using PrivateIface::TOrderedChannelFactories;

    const TOrderedContexts& getOrderedContexts() override;

    using PrivateIface::TOrderedContexts;

    const TOrderedChannelFactories& getOrderedChannelFactories() override;

    void close();

    void join();

    ~Impl() override = default;

   private:
    std::atomic<bool> closed_{false};
    std::atomic<bool> joined_{false};

    std::unordered_map<std::string, std::shared_ptr<transport::Context>>
        contexts_;
    std::unordered_map<std::string, std::shared_ptr<channel::ChannelFactory>>
        channelFactories_;

    TOrderedContexts contextsByPriority_;
    TOrderedChannelFactories channelFactoriesByPriority_;

    ClosingEmitter closingEmitter_;
  };

  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  std::shared_ptr<Impl> impl_;

  friend class Listener;
  friend class Pipe;
};

} // namespace tensorpipe
