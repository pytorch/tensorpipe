/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/core/context_impl.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/nop_types.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class ContextImpl;

class ListenerImpl final : public std::enable_shared_from_this<ListenerImpl> {
 public:
  ListenerImpl(
      std::shared_ptr<ContextImpl> context,
      std::string id,
      const std::vector<std::string>& urls);

  // Called by the listener's constructor.
  void init();

  using accept_callback_fn = Listener::accept_callback_fn;

  void accept(accept_callback_fn fn);

  const std::map<std::string, std::string>& addresses() const;

  const std::string& address(const std::string& transport) const;

  std::string url(const std::string& transport) const;

  using connection_request_callback_fn = std::function<
      void(const Error&, std::string, std::shared_ptr<transport::Connection>)>;

  uint64_t registerConnectionRequest(connection_request_callback_fn fn);
  void unregisterConnectionRequest(uint64_t registrationId);

  void close();

 private:
  void acceptFromLoop(accept_callback_fn fn);

  void closeFromLoop();

  Error error_{Error::kSuccess};

  std::shared_ptr<ContextImpl> context_;

  // An identifier for the listener, composed of the identifier for the context,
  // combined with an increasing sequence number. It will be used as a prefix
  // for the identifiers of pipes. All of them will only be used for logging and
  // debugging purposes.
  std::string id_;

  // Sequence numbers for the pipes created by this listener, used to create
  // their identifiers based off this listener's identifier. They will only be
  // used for logging and debugging.
  std::atomic<uint64_t> pipeCounter_{0};

  std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
      listeners_;
  std::map<std::string, std::string> addresses_;

  // A sequence number for the calls to accept.
  uint64_t nextPipeBeingAccepted_{0};

  // A sequence number for the invocations of the callbacks of accept.
  uint64_t nextAcceptCallbackToCall_{0};

  RearmableCallback<const Error&, std::shared_ptr<Pipe>> acceptCallback_;

  // Needed to keep them alive.
  std::unordered_set<std::shared_ptr<transport::Connection>>
      connectionsWaitingForHello_;

  uint64_t nextConnectionRequestRegistrationId_{0};

  // FIXME Consider using a (ordered) map, because keys are IDs which are
  // generated in sequence and thus we can do a quick (but partial) check of
  // whether a callback is in the map by comparing its ID with the smallest
  // and largest key, which in an ordered map are the first and last item.
  std::unordered_map<uint64_t, connection_request_callback_fn>
      connectionRequestRegistrations_;

  //
  // Initialization
  //

  void initFromLoop();

  //
  // Helpers to prepare callbacks from transports
  //

  CallbackWrapper<ListenerImpl> callbackWrapper_{*this, *this->context_};

  //
  // Error handling
  //

  void setError(Error error);

  void handleError();

  //
  // Everything else
  //

  void armListener(std::string transport);
  void onAccept(
      std::string transport,
      std::shared_ptr<transport::Connection> connection);
  void onConnectionHelloRead(
      std::string transport,
      std::shared_ptr<transport::Connection> connection,
      const Packet& nopPacketIn);

  template <typename T>
  friend class CallbackWrapper;

  // Contexts do sometimes need to call directly into closeFromLoop, in order to
  // make sure that some of their operations can happen "atomically" on the
  // connection, without possibly other operations occurring in between (e.g.,
  // an error).
  friend ContextImpl;
};

} // namespace tensorpipe
