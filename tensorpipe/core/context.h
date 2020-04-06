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
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  std::unordered_map<std::string, std::shared_ptr<transport::Context>>
      contexts_;
  std::unordered_map<std::string, std::shared_ptr<channel::ChannelFactory>>
      channelFactories_;

  std::
      map<int64_t, std::tuple<std::string, std::shared_ptr<transport::Context>>>
          contextsByPriority_;
  std::map<
      int64_t,
      std::tuple<std::string, std::shared_ptr<channel::ChannelFactory>>>
      channelFactoriesByPriority_;

  std::shared_ptr<transport::Context> getContextForTransport_(std::string);
  std::shared_ptr<channel::ChannelFactory> getChannelFactory_(std::string);

  ClosingEmitter closingEmitter_;

  friend class Listener;
  friend class Pipe;
};

} // namespace tensorpipe
