/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/connection_boilerplate.h>
#include <tensorpipe/transport/listener_boilerplate.h>

namespace tensorpipe {
namespace transport {

template <typename TCtx, typename TList, typename TConn>
class ContextImplBoilerplate : public virtual DeferredExecutor,
                               public std::enable_shared_from_this<TCtx> {
 public:
  explicit ContextImplBoilerplate(std::string domainDescriptor);

  ContextImplBoilerplate(const ContextImplBoilerplate&) = delete;
  ContextImplBoilerplate(ContextImplBoilerplate&&) = delete;
  ContextImplBoilerplate& operator=(const ContextImplBoilerplate&) = delete;
  ContextImplBoilerplate& operator=(ContextImplBoilerplate&&) = delete;

  void init();

  std::shared_ptr<Connection> connect(std::string addr);

  std::shared_ptr<Listener> listen(std::string addr);

  const std::string& domainDescriptor() const;

  // Enrolling dependent objects (listeners and connections) causes them to be
  // kept alive for as long as the context exists. These objects should enroll
  // themselves as soon as they're created (in their initImplFromLoop method)
  // and unenroll themselves after they've completed handling an error (either
  // right in the handleErrorImpl method or in a subsequent callback). The
  // context, on the other hand, should avoid terminating (i.e., complete
  // joining) until all objects have unenrolled themselves.
  void enroll(TList& listener);
  void enroll(TConn& connection);
  void unenroll(TList& listener);
  void unenroll(TConn& connection);

  // Return whether the context is in a closed state. To avoid race conditions,
  // this must be called from within the loop.
  bool closed();

  void setId(std::string id);

  void close();

  void join();

  virtual ~ContextImplBoilerplate() = default;

 protected:
  virtual void initImplFromLoop() {}
  virtual void handleErrorImpl() = 0;
  virtual void joinImpl() = 0;

  void setError(Error error);

  Error error_{Error::kSuccess};

  // An identifier for the context, composed of the identifier for the context,
  // combined with the transport's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  CallbackWrapper<TCtx> callbackWrapper_{*this, *this};

 private:
  void initFromLoop();
  void closeFromLoop();

  void handleError();

  std::atomic<bool> joined_{false};

  const std::string domainDescriptor_;

  // Sequence numbers for the listeners and connections created by this context,
  // used to create their identifiers based off this context's identifier. They
  // will only be used for logging and debugging.
  std::atomic<uint64_t> listenerCounter_{0};
  std::atomic<uint64_t> connectionCounter_{0};

  // Store shared_ptrs to dependent objects that have enrolled themselves to
  // keep them alive. We use a map, indexed by raw pointers, rather than a set
  // of shared_ptrs so that we can erase objects without them having to create
  // a fresh shared_ptr just for that.
  std::unordered_map<TList*, std::shared_ptr<TList>> listeners_;
  std::unordered_map<TConn*, std::shared_ptr<TConn>> connections_;

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::CallbackWrapper;
};

template <typename TCtx, typename TList, typename TConn>
ContextImplBoilerplate<TCtx, TList, TConn>::ContextImplBoilerplate(
    std::string domainDescriptor)
    : domainDescriptor_(std::move(domainDescriptor)) {}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::init() {
  deferToLoop([this]() { initFromLoop(); });
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::initFromLoop() {
  TP_DCHECK(inLoop());

  TP_DCHECK(!error_);

  initImplFromLoop();
}

template <typename TCtx, typename TList, typename TConn>
std::shared_ptr<Connection> ContextImplBoilerplate<TCtx, TList, TConn>::connect(
    std::string addr) {
  std::string connectionId = id_ + ".c" + std::to_string(connectionCounter_++);
  TP_VLOG(7) << "Transport context " << id_ << " is opening connection "
             << connectionId << " to address " << addr;
  return std::make_shared<ConnectionBoilerplate<TCtx, TList, TConn>>(
      typename ConnectionImplBoilerplate<TCtx, TList, TConn>::
          ConstructorToken(),
      this->shared_from_this(),
      std::move(connectionId),
      std::move(addr));
}

template <typename TCtx, typename TList, typename TConn>
std::shared_ptr<Listener> ContextImplBoilerplate<TCtx, TList, TConn>::listen(
    std::string addr) {
  std::string listenerId = id_ + ".l" + std::to_string(listenerCounter_++);
  TP_VLOG(7) << "Transport context " << id_ << " is opening listener "
             << listenerId << " on address " << addr;
  return std::make_shared<ListenerBoilerplate<TCtx, TList, TConn>>(
      typename ListenerImplBoilerplate<TCtx, TList, TConn>::ConstructorToken(),
      this->shared_from_this(),
      std::move(listenerId),
      std::move(addr));
}

template <typename TCtx, typename TList, typename TConn>
const std::string& ContextImplBoilerplate<TCtx, TList, TConn>::
    domainDescriptor() const {
  return domainDescriptor_;
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::enroll(TList& listener) {
  TP_DCHECK(inLoop());
  bool wasInserted;
  std::tie(std::ignore, wasInserted) =
      listeners_.emplace(&listener, listener.shared_from_this());
  TP_DCHECK(wasInserted);
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::enroll(TConn& connection) {
  TP_DCHECK(inLoop());
  bool wasInserted;
  std::tie(std::ignore, wasInserted) =
      connections_.emplace(&connection, connection.shared_from_this());
  TP_DCHECK(wasInserted);
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::unenroll(TList& listener) {
  TP_DCHECK(inLoop());
  auto numRemoved = listeners_.erase(&listener);
  TP_DCHECK_EQ(numRemoved, 1);
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::unenroll(TConn& connection) {
  TP_DCHECK(inLoop());
  auto numRemoved = connections_.erase(&connection);
  TP_DCHECK_EQ(numRemoved, 1);
}

template <typename TCtx, typename TList, typename TConn>
bool ContextImplBoilerplate<TCtx, TList, TConn>::closed() {
  TP_DCHECK(inLoop());
  return error_;
};

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::setId(std::string id) {
  TP_VLOG(7) << "Transport context " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::close() {
  deferToLoop([this]() { closeFromLoop(); });
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::closeFromLoop() {
  TP_DCHECK(inLoop());
  TP_VLOG(7) << "Transport context " << id_ << " is closing";
  setError(TP_CREATE_ERROR(ContextClosedError));
  TP_VLOG(7) << "Transport context " << id_ << " done closing";
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::handleError() {
  TP_DCHECK(inLoop());
  TP_VLOG(8) << "Transport context " << id_ << " is handling error "
             << error_.what();

  // Make a copy as they could unenroll themselves inline.
  auto listenersCopy = listeners_;
  auto connectionsCopy = connections_;
  // We call closeFromLoop, rather than just close, because we need these
  // objects to transition _immediately_ to error, "atomically". If we just
  // deferred closing to later, this could come after some already-enqueued
  // operations that could try to access the context, which would be closed,
  // and this could fail.
  for (auto& iter : listenersCopy) {
    iter.second->closeFromLoop();
  }
  for (auto& iter : connectionsCopy) {
    iter.second->closeFromLoop();
  }

  handleErrorImpl();
}

template <typename TCtx, typename TList, typename TConn>
void ContextImplBoilerplate<TCtx, TList, TConn>::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(7) << "Transport context " << id_ << " is joining";

    // As closing is deferred to the loop, we must wait for closeImpl to be
    // actually called before we call joinImpl, to avoid race conditions. For
    // this, we defer another task to the loop, which we know will run after the
    // closing, and then we wait for that task to be run.
    std::promise<void> hasClosed;
    deferToLoop([&]() { hasClosed.set_value(); });
    hasClosed.get_future().wait();

    joinImpl();

    TP_VLOG(7) << "Transport context " << id_ << " done joining";

    TP_DCHECK(listeners_.empty());
    TP_DCHECK(connections_.empty());
  }
}

} // namespace transport
} // namespace tensorpipe
