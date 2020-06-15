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

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class Listener;
class Pipe;

class ContextOptions {
 public:
  std::string name_;

  // The name should be a semantically meaningful description of this context.
  // It will only be used for logging and debugging purposes, to identify the
  // endpoints of a pipe.
  ContextOptions&& name(std::string name) && {
    name_ = std::move(name);
    return std::move(*this);
  }
};

class PipeOptions {
 public:
  std::string remoteName_;

  // The name should be a semantically meaningful description of the context
  // that the pipe is connecting to. It will only be used for logging and
  // debugging purposes, to identify the endpoints of a pipe.
  PipeOptions&& remoteName(std::string remoteName) && {
    remoteName_ = std::move(remoteName);
    return std::move(*this);
  }
};

class Context final {
 public:
  explicit Context(ContextOptions opts = ContextOptions());

  void registerTransport(
      int64_t,
      std::string,
      std::shared_ptr<transport::Context>);

  void registerChannel(int64_t, std::string, std::shared_ptr<channel::Context>);

  std::shared_ptr<Listener> listen(const std::vector<std::string>&);

  std::shared_ptr<Pipe> connect(
      const std::string&,
      PipeOptions opts = PipeOptions());

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

    // Return the name given to the context's constructor. It will be retrieved
    // by the pipes and listener in order to attach it to logged messages.
    virtual const std::string& getName() = 0;

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
  // Allow pipe to see the private interface.
  friend class Pipe;
};

} // namespace tensorpipe
