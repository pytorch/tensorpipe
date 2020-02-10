/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
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
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
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

  std::thread callbackCaller_;
  Queue<optional<std::function<void()>>> callbackQueue_;

  void start_();
  void runCallbackCaller_();
  void callCallback_(std::function<void()>);

  friend class Listener;
  friend class Pipe;
};

} // namespace tensorpipe
