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
#include <tensorpipe/proto/core.pb.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {

class Pipe;

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

  // Put the listener in a terminal state, aborting its pending operations and
  // rejecting future ones, and release its resrouces. This may be carried out
  // asynchronously, in background. Since the pipes may occasionally use the
  // listener to open new connections, closing a listener may trigger errors
  // in the pipes.
  void close();

  ~Listener();

 private:
  class PrivateIface {
   public:
    using connection_request_callback_fn = std::function<void(
        const Error&,
        std::string,
        std::shared_ptr<transport::Connection>)>;

    virtual uint64_t registerConnectionRequest(
        connection_request_callback_fn) = 0;

    virtual void unregisterConnectionRequest(uint64_t) = 0;

    virtual const std::map<std::string, std::string>& addresses() const = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl : public PrivateIface, public std::enable_shared_from_this<Impl> {
    // Use the passkey idiom to allow make_shared to call what should be a
    // private constructor. See https://abseil.io/tips/134 for more information.
    struct ConstructorToken {};

   public:
    static std::shared_ptr<Impl> create(
        std::shared_ptr<Context>,
        const std::vector<std::string>&);

    Impl(
        ConstructorToken,
        std::shared_ptr<Context>,
        const std::vector<std::string>&);

    void accept(accept_callback_fn);

    const std::map<std::string, std::string>& addresses() const override;

    const std::string& address(const std::string& transport) const;

    std::string url(const std::string& transport) const;

    using PrivateIface::connection_request_callback_fn;

    uint64_t registerConnectionRequest(connection_request_callback_fn) override;
    void unregisterConnectionRequest(uint64_t) override;

    void close();

    ~Impl() override = default;

   private:
    mutable std::mutex mutex_;
    std::thread::id currentLoop_{std::thread::id()};
    std::deque<std::function<void()>> pendingTasks_;

    bool inLoop_();

    void deferToLoop_(std::function<void()> fn);

    void acceptFromLoop_(accept_callback_fn);

    void closeFromLoop_();

    Error error_;

    std::shared_ptr<Context> context_;
    std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
        listeners_;
    std::map<std::string, transport::address_t> addresses_;
    LocklessRearmableCallback<
        std::function<void(
            const Error&,
            std::string,
            std::shared_ptr<transport::Connection>)>,
        const Error&,
        std::string,
        std::shared_ptr<transport::Connection>>
        acceptCallback_;

    // Needed to keep them alive.
    std::unordered_set<std::shared_ptr<transport::Connection>>
        connectionsWaitingForHello_;

    // This is atomic because it may be accessed from outside the loop.
    std::atomic<uint64_t> nextConnectionRequestRegistrationId_{0};

    // FIXME Consider using a (ordered) map, because keys are IDs which are
    // generated in sequence and thus we can do a quick (but partial) check of
    // whether a callback is in the map by comparing its ID with the smallest
    // and largest key, which in an ordered map are the first and last item.
    std::unordered_map<uint64_t, connection_request_callback_fn>
        connectionRequestRegistrations_;

    ClosingReceiver closingReceiver_;

    //
    // Initialization
    //

    void start_();

    void startFromLoop_();

    //
    // Entry points for internal code
    //

    void registerConnectionRequestFromLoop_(
        uint64_t,
        connection_request_callback_fn);

    void unregisterConnectionRequestFromLoop_(uint64_t);

    //
    // Helpers to prepare callbacks from transports
    //

    DeferringCallbackWrapper<Impl> readPacketCallbackWrapper_;
    DeferringCallbackWrapper<Impl, std::shared_ptr<transport::Connection>>
        acceptCallbackWrapper_;

    //
    // Helpers to schedule our callbacks into user code
    //

    void triggerAcceptCallback_(
        accept_callback_fn,
        const Error&,
        std::string,
        std::shared_ptr<transport::Connection>);

    void triggerConnectionRequestCallback_(
        connection_request_callback_fn,
        const Error&,
        std::string,
        std::shared_ptr<transport::Connection>);

    //
    // Error handling
    //

    void handleError_();

    //
    // Everything else
    //

    void armListener_(std::string);
    void onAccept_(std::string, std::shared_ptr<transport::Connection>);
    void onConnectionHelloRead_(
        std::string,
        std::shared_ptr<transport::Connection>,
        const proto::Packet&);

    template <typename T, typename... Args>
    friend class DeferringCallbackWrapper;
  };

  // Using a shared_ptr allows us to detach the lifetime of the implementation
  // from the public object's one and perform the destruction asynchronously.
  std::shared_ptr<Impl> impl_;

  friend class Context;
  friend class Pipe;
};

} // namespace tensorpipe
