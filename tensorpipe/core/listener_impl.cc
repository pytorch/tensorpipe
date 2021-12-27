/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/listener_impl.h>

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/context_impl.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/nop_types.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/core/pipe_impl.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {

ListenerImpl::ListenerImpl(
    std::shared_ptr<ContextImpl> context,
    std::string id,
    const std::vector<std::string>& urls)
    : context_(std::move(context)), id_(std::move(id)) {
  for (const auto& url : urls) {
    std::string transport;
    std::string address;
    std::tie(transport, address) = splitSchemeOfURL(url);
    std::shared_ptr<transport::Context> context =
        context_->getTransport(transport);
    std::shared_ptr<transport::Listener> listener = context->listen(address);
    listener->setId(id_ + ".tr_" + transport);
    addresses_.emplace(transport, listener->addr());
    listeners_.emplace(transport, std::move(listener));
  }
}

void ListenerImpl::init() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->initFromLoop(); });
}

void ListenerImpl::initFromLoop() {
  TP_DCHECK(context_->inLoop());

  if (context_->closed()) {
    // Set the error without calling setError because we do not want to invoke
    // handleError as it would find itself in a weird state (since the rest of
    // initFromLoop wouldn't have been called).
    error_ = TP_CREATE_ERROR(ListenerClosedError);
    TP_VLOG(1) << "Listener " << id_ << " is closing (without initing)";
    return;
  }

  context_->enroll(*this);

  for (const auto& listener : listeners_) {
    armListener(listener.first);
  }
}

void ListenerImpl::close() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->closeFromLoop(); });
}

void ListenerImpl::closeFromLoop() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(1) << "Listener " << id_ << " is closing";
  setError(TP_CREATE_ERROR(ListenerClosedError));
}

//
// Entry points for user code
//

void ListenerImpl::accept(accept_callback_fn fn) {
  context_->deferToLoop(
      [impl{this->shared_from_this()}, fn{std::move(fn)}]() mutable {
        impl->acceptFromLoop(std::move(fn));
      });
}

void ListenerImpl::acceptFromLoop(accept_callback_fn fn) {
  TP_DCHECK(context_->inLoop());

  uint64_t sequenceNumber = nextPipeBeingAccepted_++;
  TP_VLOG(1) << "Listener " << id_ << " received an accept request (#"
             << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error, std::shared_ptr<Pipe> pipe) {
    TP_DCHECK_EQ(sequenceNumber, nextAcceptCallbackToCall_++);
    TP_VLOG(1) << "Listener " << id_ << " is calling an accept callback (#"
               << sequenceNumber << ")";
    fn(error, std::move(pipe));
    TP_VLOG(1) << "Listener " << id_ << " done calling an accept callback (#"
               << sequenceNumber << ")";
  };

  if (error_) {
    fn(error_, std::shared_ptr<Pipe>());
    return;
  }

  acceptCallback_.arm(std::move(fn));
}

const std::map<std::string, std::string>& ListenerImpl::addresses() const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return addresses_;
}

const std::string& ListenerImpl::address(const std::string& transport) const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  const auto it = addresses_.find(transport);
  TP_THROW_ASSERT_IF(it == addresses_.end())
      << ": transport '" << transport << "' not in use by this listener.";
  return it->second;
}

std::string ListenerImpl::url(const std::string& transport) const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return transport + "://" + address(transport);
}

//
// Entry points for internal code
//

uint64_t ListenerImpl::registerConnectionRequest(
    connection_request_callback_fn fn) {
  TP_DCHECK(context_->inLoop());

  uint64_t registrationId = nextConnectionRequestRegistrationId_++;

  TP_VLOG(1) << "Listener " << id_
             << " received a connection request registration (#"
             << registrationId << ")";

  fn = [this, registrationId, fn{std::move(fn)}](
           const Error& error,
           std::string transport,
           std::shared_ptr<transport::Connection> connection) {
    TP_VLOG(1) << "Listener " << id_
               << " is calling a connection request registration callback (#"
               << registrationId << ")";
    fn(error, std::move(transport), std::move(connection));
    TP_VLOG(1) << "Listener " << id_
               << " done calling a connection request registration callback (#"
               << registrationId << ")";
  };

  if (error_) {
    fn(error_, std::string(), std::shared_ptr<transport::Connection>());
  } else {
    connectionRequestRegistrations_.emplace(registrationId, std::move(fn));
  }

  return registrationId;
}

void ListenerImpl::unregisterConnectionRequest(uint64_t registrationId) {
  TP_DCHECK(context_->inLoop());

  TP_VLOG(1) << "Listener " << id_
             << " received a connection request de-registration (#"
             << registrationId << ")";

  connectionRequestRegistrations_.erase(registrationId);
}

//
// Error handling
//

void ListenerImpl::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void ListenerImpl::handleError() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(2) << "Listener " << id_ << " is handling error " << error_.what();

  acceptCallback_.triggerAll([&]() {
    return std::make_tuple(std::cref(error_), std::shared_ptr<Pipe>());
  });
  for (auto& iter : connectionRequestRegistrations_) {
    connection_request_callback_fn fn = std::move(iter.second);
    fn(error_, std::string(), std::shared_ptr<transport::Connection>());
  }
  connectionRequestRegistrations_.clear();

  for (const auto& listener : listeners_) {
    listener.second->close();
  }

  for (const auto& connection : connectionsWaitingForHello_) {
    connection->close();
  }
  connectionsWaitingForHello_.clear();

  context_->unenroll(*this);
}

//
// Everything else
//

void ListenerImpl::onAccept(
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(context_->inLoop());
  // Keep it alive until we figure out what to do with it.
  connectionsWaitingForHello_.insert(connection);
  auto nopHolderIn = std::make_shared<NopHolder<Packet>>();
  TP_VLOG(3) << "Listener " << id_
             << " is reading nop object (spontaneous or requested connection)";
  connection->read(
      *nopHolderIn,
      callbackWrapper_([nopHolderIn,
                        transport{std::move(transport)},
                        connection](ListenerImpl& impl) mutable {
        TP_VLOG(3)
            << "Listener " << impl.id_
            << " done reading nop object (spontaneous or requested connection)";
        if (impl.error_) {
          return;
        }
        impl.connectionsWaitingForHello_.erase(connection);
        impl.onConnectionHelloRead(
            std::move(transport),
            std::move(connection),
            nopHolderIn->getObject());
      }));
}

void ListenerImpl::armListener(std::string transport) {
  TP_DCHECK(context_->inLoop());
  auto iter = listeners_.find(transport);
  if (iter == listeners_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  auto transportListener = iter->second;
  TP_VLOG(3) << "Listener " << id_ << " is accepting connection on transport "
             << transport;
  transportListener->accept(
      callbackWrapper_([transport](
                           ListenerImpl& impl,
                           std::shared_ptr<transport::Connection> connection) {
        TP_VLOG(3) << "Listener " << impl.id_
                   << " done accepting connection on transport " << transport;
        if (impl.error_) {
          return;
        }
        impl.onAccept(transport, std::move(connection));
        impl.armListener(transport);
      }));
}

void ListenerImpl::onConnectionHelloRead(
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    const Packet& nopPacketIn) {
  TP_DCHECK(context_->inLoop());
  if (nopPacketIn.is<SpontaneousConnection>()) {
    const SpontaneousConnection& nopSpontaneousConnection =
        *nopPacketIn.get<SpontaneousConnection>();
    TP_VLOG(3) << "Listener " << id_ << " got spontaneous connection";
    std::string pipeId = id_ + ".p" + std::to_string(pipeCounter_++);
    TP_VLOG(1) << "Listener " << id_ << " is opening pipe " << pipeId;
    const std::string& remoteContextName = nopSpontaneousConnection.contextName;
    if (remoteContextName != "") {
      std::string aliasPipeId = id_ + "_from_" + remoteContextName;
      TP_VLOG(1) << "Pipe " << pipeId << " aliased as " << aliasPipeId;
      pipeId = std::move(aliasPipeId);
    }
    auto pipe = std::make_shared<PipeImpl>(
        context_,
        shared_from_this(),
        std::move(pipeId),
        remoteContextName,
        std::move(transport),
        std::move(connection));
    // We initialize the pipe from the loop immediately, inline, because the
    // initialization of a pipe accepted by a listener happens partly in the
    // listener and partly in the pipe's initFromLoop, and we need these two
    // steps to happen "atomically" to make it impossible for an error to occur
    // in between.
    pipe->initFromLoop();
    acceptCallback_.trigger(
        Error::kSuccess,
        std::make_shared<Pipe>(Pipe::ConstructorToken(), std::move(pipe)));
  } else if (nopPacketIn.is<RequestedConnection>()) {
    const RequestedConnection& nopRequestedConnection =
        *nopPacketIn.get<RequestedConnection>();
    uint64_t registrationId = nopRequestedConnection.registrationId;
    TP_VLOG(3) << "Listener " << id_ << " got requested connection (#"
               << registrationId << ")";
    auto iter = connectionRequestRegistrations_.find(registrationId);
    // The connection request may have already been deregistered, for example
    // because the pipe may have been closed.
    if (iter != connectionRequestRegistrations_.end()) {
      auto fn = std::move(iter->second);
      connectionRequestRegistrations_.erase(iter);
      fn(Error::kSuccess, std::move(transport), std::move(connection));
    }
  } else {
    TP_LOG_ERROR() << "packet contained unknown content: "
                   << nopPacketIn.index();
  }
}

} // namespace tensorpipe
