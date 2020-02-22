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
  using connection_request_callback_fn = std::function<
      void(const Error&, std::string, std::shared_ptr<transport::Connection>)>;

  mutable std::mutex mutex_;
  Error error_;

  std::shared_ptr<Context> context_;
  std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
      listeners_;
  std::map<std::string, transport::address_t> addresses_;
  RearmableCallback<accept_callback_fn, const Error&, std::shared_ptr<Pipe>>
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
  // Entry points for callbacks from transports and listener
  // and helpers to prepare them
  //

  using transport_read_packet_callback_fn =
      transport::Connection::read_proto_callback_fn<proto::Packet>;
  using bound_read_packet_callback_fn =
      std::function<void(Listener&, const proto::Packet&)>;
  transport_read_packet_callback_fn wrapReadPacketCallback_(
      bound_read_packet_callback_fn = nullptr);
  void readPacketCallbackEntryPoint_(
      bound_read_packet_callback_fn,
      const Error&,
      const proto::Packet&);

  using transport_accept_callback_fn =
      std::function<void(const Error&, std::shared_ptr<transport::Connection>)>;
  using bound_accept_callback_fn =
      std::function<void(Listener&, std::shared_ptr<transport::Connection>)>;
  transport_accept_callback_fn wrapAcceptCallback_(
      bound_accept_callback_fn = nullptr);
  void acceptCallbackEntryPoint_(
      bound_accept_callback_fn,
      const Error&,
      std::shared_ptr<transport::Connection>);

  //
  // Helpers to schedule our callbacks into user code
  //

  void triggerAcceptCallback_(
      accept_callback_fn,
      const Error&,
      std::shared_ptr<Pipe>);

  void triggerConnectionRequestCallback_(
      connection_request_callback_fn,
      const Error&,
      std::string,
      std::shared_ptr<transport::Connection>);

  //
  // Error handling
  //

  void flushEverythingOnError_();

  //
  // Everything else
  //

  void armListener_(std::string);
  void onAccept_(std::string, std::shared_ptr<transport::Connection>);
  void onConnectionHelloRead_(
      std::string,
      std::shared_ptr<transport::Connection>,
      const proto::Packet&);

  friend class Context;
  friend class Pipe;
};

} // namespace tensorpipe
