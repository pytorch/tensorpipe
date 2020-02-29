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
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class Context final {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Context> create();

  explicit Context(ConstructorToken);

  ~Context();

  void registerTransport(
      int64_t,
      std::string,
      std::shared_ptr<transport::Context>);

  void registerChannelFactory(
      int64_t,
      std::string,
      std::shared_ptr<channel::ChannelFactory>);

  void join();

 private:
  std::atomic<bool> done_{false};

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

  friend class Listener;
  friend class Pipe;
};

} // namespace tensorpipe
