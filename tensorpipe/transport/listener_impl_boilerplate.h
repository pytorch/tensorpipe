/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/connection_boilerplate.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace transport {

template <typename TCtx, typename TList, typename TConn>
class ListenerImplBoilerplate : public std::enable_shared_from_this<TList> {
 public:
  class ConstructorToken {
   public:
    ConstructorToken(const ConstructorToken&) = default;

   private:
    explicit ConstructorToken() {}
    friend ContextImplBoilerplate<TCtx, TList, TConn>;
    friend ListenerImplBoilerplate<TCtx, TList, TConn>;
  };

  ListenerImplBoilerplate(
      ConstructorToken token,
      std::shared_ptr<TCtx> context,
      std::string id);

  ListenerImplBoilerplate(const ListenerImplBoilerplate&) = delete;
  ListenerImplBoilerplate(ListenerImplBoilerplate&&) = delete;
  ListenerImplBoilerplate& operator=(const ListenerImplBoilerplate&) = delete;
  ListenerImplBoilerplate& operator=(ListenerImplBoilerplate&&) = delete;

  // Initialize member fields that need `shared_from_this`.
  void init();

  // Queue a callback to be called when a connection comes in.
  using accept_callback_fn = Listener::accept_callback_fn;
  void accept(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addr() const;

  // Tell the listener what its identifier is.
  void setId(std::string id);

  // Shut down the listener and its resources.
  void close();

  virtual ~ListenerImplBoilerplate() = default;

 protected:
  virtual void initImplFromLoop() = 0;
  virtual void acceptImplFromLoop(accept_callback_fn fn) = 0;
  virtual std::string addrImplFromLoop() const = 0;
  virtual void handleErrorImpl() = 0;

  void setError(Error error);

  const std::shared_ptr<TCtx> context_;

  Error error_{Error::kSuccess};

  template <typename... Args>
  std::shared_ptr<Connection> createAndInitConnection(Args&&... args);

  // An identifier for the listener, composed of the identifier for the context,
  // combined with an increasing sequence number. It will be used as a prefix
  // for the identifiers of connections. All of them will only be used for
  // logging and debugging purposes.
  std::string id_;

 private:
  // Initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Queue a callback to be called when a connection comes in.
  void acceptFromLoop(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addrFromLoop() const;

  void setIdFromLoop(std::string id);

  // Shut down the connection and its resources.
  void closeFromLoop();

  // Deal with an error.
  void handleError();

  // A sequence number for the calls to accept.
  uint64_t nextConnectionBeingAccepted_{0};

  // Sequence numbers for the connections created by this listener, used to
  // create their identifiers based off this listener's identifier. They will
  // only be used for logging and debugging.
  std::atomic<uint64_t> connectionCounter_{0};

  // Contexts do sometimes need to call directly into closeFromLoop, in order to
  // make sure that some of their operations can happen "atomically" on the
  // connection, without possibly other operations occurring in between (e.g.,
  // an error).
  friend ContextImplBoilerplate<TCtx, TList, TConn>;
};

template <typename TCtx, typename TList, typename TConn>
ListenerImplBoilerplate<TCtx, TList, TConn>::ListenerImplBoilerplate(
    ConstructorToken /* unused */,
    std::shared_ptr<TCtx> context,
    std::string id)
    : context_(std::move(context)), id_(std::move(id)) {}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::init() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->initFromLoop(); });
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::initFromLoop() {
  if (context_->closed()) {
    // Set the error without calling setError because we do not want to invoke
    // the subclass's handleErrorImpl as it would find itself in a weird state
    // (since initFromLoop wouldn't have been called).
    error_ = TP_CREATE_ERROR(ListenerClosedError);
    TP_VLOG(7) << "Listener " << id_ << " is closing (without initing)";
    return;
  }

  initImplFromLoop();
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::accept(
    accept_callback_fn fn) {
  context_->deferToLoop(
      [impl{this->shared_from_this()}, fn{std::move(fn)}]() mutable {
        impl->acceptFromLoop(std::move(fn));
      });
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::acceptFromLoop(
    accept_callback_fn fn) {
  TP_DCHECK(context_->inLoop());

  uint64_t sequenceNumber = nextConnectionBeingAccepted_++;
  TP_VLOG(7) << "Listener " << id_ << " received an accept request (#"
             << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, std::shared_ptr<Connection> connection) {
    TP_VLOG(7) << "Listener " << id_ << " is calling an accept callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(connection));
    TP_VLOG(7) << "Listener " << id_ << " done calling an accept callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    fn(error_, std::shared_ptr<Connection>());
    return;
  }

  acceptImplFromLoop(std::move(fn));
}

template <typename TCtx, typename TList, typename TConn>
std::string ListenerImplBoilerplate<TCtx, TList, TConn>::addr() const {
  std::string addr;
  context_->runInLoop([this, &addr]() { addr = addrFromLoop(); });
  return addr;
}

template <typename TCtx, typename TList, typename TConn>
std::string ListenerImplBoilerplate<TCtx, TList, TConn>::addrFromLoop() const {
  TP_DCHECK(context_->inLoop());

  return addrImplFromLoop();
}

template <typename TCtx, typename TList, typename TConn>
template <typename... Args>
std::shared_ptr<Connection> ListenerImplBoilerplate<TCtx, TList, TConn>::
    createAndInitConnection(Args&&... args) {
  TP_DCHECK(context_->inLoop());
  std::string connectionId = id_ + ".c" + std::to_string(connectionCounter_++);
  TP_VLOG(7) << "Listener " << id_ << " is opening connection " << connectionId;
  auto connection = std::make_shared<TConn>(
      typename ConnectionImplBoilerplate<TCtx, TList, TConn>::
          ConstructorToken(),
      context_,
      std::move(connectionId),
      std::forward<Args>(args)...);
  // We initialize the connection from the loop immediately, inline, because the
  // initialization of a connection accepted by a listener typically happens
  // partly in the listener (e.g., opening and accepting the socket) and partly
  // in the connection's initFromLoop, and we need these two steps to happen
  // "atomicically" to make it impossible for an error to occur in between.
  connection->initFromLoop();
  return std::make_shared<ConnectionBoilerplate<TCtx, TList, TConn>>(
      std::move(connection));
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::setId(std::string id) {
  context_->deferToLoop(
      [impl{this->shared_from_this()}, id{std::move(id)}]() mutable {
        impl->setIdFromLoop(std::move(id));
      });
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::setIdFromLoop(
    std::string id) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(7) << "Listener " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::close() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->closeFromLoop(); });
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::closeFromLoop() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(7) << "Listener " << id_ << " is closing";
  setError(TP_CREATE_ERROR(ListenerClosedError));
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

template <typename TCtx, typename TList, typename TConn>
void ListenerImplBoilerplate<TCtx, TList, TConn>::handleError() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(8) << "Listener " << id_ << " is handling error " << error_.what();

  handleErrorImpl();
}

} // namespace transport
} // namespace tensorpipe
