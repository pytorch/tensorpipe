/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <memory>
#include <unordered_set>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {

// The listener.
//
// Listeners are used to produce pipes. Depending on the type of the
// context, listeners may use a variety of addresses to listen on. For
// example, for TCP/IP sockets they listen on an IPv4 or IPv6 address,
// for Unix domain sockets they listen on a path, etcetera.
//
// A pipe can only be accepted from this listener after it has been
// fully established. This means that both its connection and all its
// side channels have been established.
//
class Listener final : public std::enable_shared_from_this<Listener> {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Listener> create(
      std::shared_ptr<Context>,
      const std::vector<std::string>&);

  Listener(
      ConstructorToken,
      std::shared_ptr<Context>,
      std::unordered_map<std::string, std::shared_ptr<transport::Listener>>);

  using accept_callback_fn =
      std::function<void(const Error&, std::shared_ptr<Pipe>)>;

  void accept(accept_callback_fn);

 private:
  std::shared_ptr<Context> context_;
  std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
      listeners_;
  RearmableCallback<accept_callback_fn, const Error&, std::shared_ptr<Pipe>>
      acceptCallback_;

  void start_();
  void armListener_(std::string);
  void onAccept_(std::string, std::shared_ptr<transport::Connection>);
  void onConnectionHelloRead_(
      std::string,
      std::shared_ptr<transport::Connection>,
      const void*,
      size_t);

  //
  // Utilities for internal components to say they expect an incoming connection
  //

  // Needed to keep them alive.
  std::unordered_set<std::shared_ptr<transport::Connection>>
      connectionsWaitingForHello_;

  uint64_t nextConnectionRequestRegistrationId_{0};

  using connection_request_callback_fn =
      std::function<void(std::string, std::shared_ptr<transport::Connection>)>;

  uint64_t registerConnectionRequest_(connection_request_callback_fn);

  void unregisterConnectionRequest_(uint64_t);

  // FIXME Consider using a (ordered) map, because keys are IDs which are
  // generated in sequence and thus we can do a quick (but partial) check of
  // whether a callback is in the map by comparing its ID with the smallest and
  // largest key, which in an ordered map are the first and last item.
  std::unordered_map<uint64_t, connection_request_callback_fn>
      connectionRequestRegistrations_;

  friend class Context;
};

} // namespace tensorpipe
