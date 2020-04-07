/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <tensorpipe/channel/channel.h>
#include <tensorpipe/common/callback.h>
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

  void registerChannel(int64_t, std::string, std::shared_ptr<channel::Context>);

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

    virtual std::shared_ptr<transport::Context> getTransport(
        const std::string&) = 0;

    virtual std::shared_ptr<channel::Context> getChannel(
        const std::string&) = 0;

    using TOrderedTransports = std::map<
        int64_t,
        std::tuple<std::string, std::shared_ptr<transport::Context>>>;

    virtual const TOrderedTransports& getOrderedTransports() = 0;

    using TOrderedChannels = std::map<
        int64_t,
        std::tuple<std::string, std::shared_ptr<channel::Context>>>;

    virtual const TOrderedChannels& getOrderedChannels() = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl;

  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  std::shared_ptr<Impl> impl_;

  friend class Listener;
  friend class Pipe;
};

} // namespace tensorpipe
