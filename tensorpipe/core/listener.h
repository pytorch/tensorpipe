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
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/context.h>
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
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Listener> create(
      std::shared_ptr<Context>,
      const std::vector<std::string>&);

  Listener(
      ConstructorToken,
      std::shared_ptr<Context>,
      const std::vector<std::string>&);

  //
  // Entry points for user code
  //

  using accept_callback_fn =
      std::function<void(const Error&, std::shared_ptr<Pipe>)>;

  void accept(accept_callback_fn);

  // Returns map with the materialized address of listeners by transport.
  //
  // If you don't bind a transport listener to a specific port or address, it
  // may generate its address automatically. Then, in order to connect to the
  // listener, the user must use a separate mechanism to communicate the
  // materialized address to whoever wants to connect.
  //
  const std::map<std::string, std::string>& addresses() const;

  // Returns materialized address for specific transport.
  //
  // See `addresses()` for more information.
  //
  const std::string& address(const std::string& transport) const;

  // Returns URL with materialized address for specific transport.
  //
  // See `addresses()` for more information.
  //
  std::string url(const std::string& transport) const;

 private:
  // Each time a thread starts running some of the listener's code, we acquire
  // this mutex. There are two "entry points" where control is handed to the
  // listener: the public user-facing functions, and the callbacks (which we
  // always wrap with wrapFooCallback_, which first calls fooEntryPoint_, which
  // is where the mutex is acquired). Some internal methods may however want to
  // temporarily release the lock, so we give all of them a reference to the
  // lock that has been acquired at the entry point.
  mutable std::mutex mutex_;
  using TLock = std::unique_lock<std::mutex>&;

  using connection_request_callback_fn = std::function<
      void(const Error&, std::string, std::shared_ptr<transport::Connection>)>;

  Error error_;

  std::shared_ptr<Context> context_;
  std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
      listeners_;
  std::map<std::string, transport::address_t> addresses_;
  RearmableCallbackWithExternalLock<
      std::function<void(const Error&, std::shared_ptr<Pipe>, TLock)>,
      const Error&,
      std::shared_ptr<Pipe>>
      acceptCallback_;

  // Needed to keep them alive.
  std::unordered_set<std::shared_ptr<transport::Connection>>
      connectionsWaitingForHello_;

  uint64_t nextConnectionRequestRegistrationId_{0};

  // FIXME Consider using a (ordered) map, because keys are IDs which are
  // generated in sequence and thus we can do a quick (but partial) check of
  // whether a callback is in the map by comparing its ID with the smallest and
  // largest key, which in an ordered map are the first and last item.
  std::unordered_map<uint64_t, connection_request_callback_fn>
      connectionRequestRegistrations_;

  //
  // Initialization
  //

  void start_();

  //
  // Entry points for internal code
  //

  uint64_t registerConnectionRequest_(connection_request_callback_fn);

  void unregisterConnectionRequest_(uint64_t);

  //
  // Helpers to prepare callbacks from transports
  //

  CallbackWrapper<Listener> readPacketCallbackWrapper_;
  CallbackWrapper<Listener, std::shared_ptr<transport::Connection>>
      acceptCallbackWrapper_;

  //
  // Helpers to schedule our callbacks into user code
  //

  void triggerAcceptCallback_(
      accept_callback_fn,
      const Error&,
      std::shared_ptr<Pipe>,
      TLock);

  void triggerConnectionRequestCallback_(
      connection_request_callback_fn,
      const Error&,
      std::string,
      std::shared_ptr<transport::Connection>,
      TLock);

  //
  // Error handling
  //

  void handleError_(TLock);

  //
  // Everything else
  //

  void armListener_(std::string, TLock);
  void onAccept_(std::string, std::shared_ptr<transport::Connection>, TLock);
  void onConnectionHelloRead_(
      std::string,
      std::shared_ptr<transport::Connection>,
      const proto::Packet&,
      TLock);

  friend class Context;
  friend class Pipe;
  template <typename T, typename... Args>
  friend class CallbackWrapper;
};

} // namespace tensorpipe
