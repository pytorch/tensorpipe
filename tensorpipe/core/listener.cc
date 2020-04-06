/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/listener.h>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/proto/core.pb.h>

namespace tensorpipe {

std::shared_ptr<Listener> Listener::create(
    std::shared_ptr<Context> context,
    const std::vector<std::string>& urls) {
  auto listener =
      std::make_shared<Listener>(ConstructorToken(), std::move(context), urls);
  listener->start_();
  return listener;
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    const std::vector<std::string>& urls)
    : context_(std::move(context)),
      closingReceiver_(context_, context_->closingEmitter_),
      readPacketCallbackWrapper_(*this),
      acceptCallbackWrapper_(*this) {
  for (const auto& url : urls) {
    std::string transport;
    std::string address;
    std::tie(transport, address) = splitSchemeOfURL(url);
    auto iter = context_->contexts_.find(transport);
    if (iter == context_->contexts_.end()) {
      TP_THROW_EINVAL() << "unsupported transport " << transport;
    }
    transport::Context& context = *(iter->second);
    std::shared_ptr<transport::Listener> listener = context.listen(address);
    addresses_.emplace(transport, listener->addr());
    listeners_.emplace(transport, std::move(listener));
  }
}

void Listener::start_() {
  deferToLoop_([this]() { startFromLoop_(); });
}

void Listener::startFromLoop_() {
  TP_DCHECK(inLoop_());
  closingReceiver_.activate(*this);
  for (const auto& listener : listeners_) {
    armListener_(listener.first);
  }
}

bool Listener::inLoop_() {
  return currentLoop_ == std::this_thread::get_id();
}

void Listener::deferToLoop_(std::function<void()> fn) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    pendingTasks_.push_back(std::move(fn));
    if (currentLoop_ != std::thread::id()) {
      return;
    }
    currentLoop_ = std::this_thread::get_id();
  }

  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (pendingTasks_.empty()) {
        currentLoop_ = std::thread::id();
        return;
      }
      task = std::move(pendingTasks_.front());
      pendingTasks_.pop_front();
    }
    task();
  }
}

void Listener::close() {
  deferToLoop_([this]() { closeFromLoop_(); });
}

void Listener::closeFromLoop_() {
  TP_DCHECK(inLoop_());

  if (!error_) {
    error_ = TP_CREATE_ERROR(ListenerClosedError);
    handleError_();
  }
}

Listener::~Listener() {
  close();
}

//
// Entry points for user code
//

void Listener::accept(accept_callback_fn fn) {
  deferToLoop_(
      [this, fn{std::move(fn)}]() mutable { acceptFromLoop_(std::move(fn)); });
}

void Listener::acceptFromLoop_(accept_callback_fn fn) {
  TP_DCHECK(inLoop_());

  if (error_) {
    triggerAcceptCallback_(
        std::move(fn),
        error_,
        std::string(),
        std::shared_ptr<transport::Connection>());
    return;
  }

  acceptCallback_.arm(runIfAlive(
      *this,
      std::function<void(
          Listener&,
          const Error&,
          std::string,
          std::shared_ptr<transport::Connection>)>(
          [fn{std::move(fn)}](
              Listener& listener,
              const Error& error,
              std::string transport,
              std::shared_ptr<transport::Connection> connection) mutable {
            listener.triggerAcceptCallback_(
                std::move(fn),
                error,
                std::move(transport),
                std::move(connection));
          })));
}

const std::map<std::string, std::string>& Listener::addresses() const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return addresses_;
}

const std::string& Listener::address(const std::string& transport) const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  const auto it = addresses_.find(transport);
  TP_THROW_ASSERT_IF(it == addresses_.end())
      << ": transport '" << transport << "' not in use by this listener.";
  return it->second;
}

std::string Listener::url(const std::string& transport) const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return transport + "://" + address(transport);
}

//
// Entry points for internal code
//

uint64_t Listener::registerConnectionRequest_(
    connection_request_callback_fn fn) {
  // We cannot return a value if we defer the function. Thus we obtain an ID
  // now (and this is why the next ID is an atomic), return it, and defer the
  // rest of the processing.
  // FIXME Avoid this hack by doing like we did with the channels' recv: have
  // this accept a callback that is called with the registration ID.
  uint64_t registrationId = nextConnectionRequestRegistrationId_++;
  deferToLoop_([this, registrationId, fn{std::move(fn)}]() mutable {
    registerConnectionRequestFromLoop_(registrationId, std::move(fn));
  });
  return registrationId;
}

void Listener::registerConnectionRequestFromLoop_(
    uint64_t registrationId,
    connection_request_callback_fn fn) {
  TP_DCHECK(inLoop_());

  if (error_) {
    triggerConnectionRequestCallback_(
        std::move(fn),
        error_,
        std::string(),
        std::shared_ptr<transport::Connection>());
  } else {
    connectionRequestRegistrations_.emplace(registrationId, std::move(fn));
  }
}

void Listener::unregisterConnectionRequest_(uint64_t registrationId) {
  deferToLoop_([this, registrationId]() {
    unregisterConnectionRequestFromLoop_(registrationId);
  });
}

void Listener::unregisterConnectionRequestFromLoop_(uint64_t registrationId) {
  TP_DCHECK(inLoop_());
  connectionRequestRegistrations_.erase(registrationId);
}

//
// Helpers to schedule our callbacks into user code
//

void Listener::triggerAcceptCallback_(
    accept_callback_fn fn,
    const Error& error,
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(inLoop_());
  // Create the pipe here, without holding the lock, as otherwise TSAN would
  // report a false positive lock order inversion.
  // FIXME Simplify this now that we don't use locks anymore.
  std::shared_ptr<Pipe> pipe;
  if (!error) {
    pipe = std::make_shared<Pipe>(
        Pipe::ConstructorToken(),
        context_,
        shared_from_this(),
        std::move(transport),
        std::move(connection));
  }
  fn(error, std::move(pipe));
}

void Listener::triggerConnectionRequestCallback_(
    connection_request_callback_fn fn,
    const Error& error,
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(inLoop_());
  // FIXME Avoid this function now that we don't use locks anymore.
  fn(error, std::move(transport), std::move(connection));
}

//
// Error handling
//

void Listener::handleError_() {
  TP_DCHECK(inLoop_());

  acceptCallback_.triggerAll([&]() {
    return std::make_tuple(
        error_, std::string(), std::shared_ptr<transport::Connection>());
  });
  for (auto& iter : connectionRequestRegistrations_) {
    connection_request_callback_fn fn = std::move(iter.second);
    triggerConnectionRequestCallback_(
        std::move(fn),
        error_,
        std::string(),
        std::shared_ptr<transport::Connection>());
  }
  connectionRequestRegistrations_.clear();

  for (const auto& listener : listeners_) {
    listener.second->close();
  }
}

//
// Everything else
//

void Listener::onAccept_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(inLoop_());
  // Keep it alive until we figure out what to do with it.
  connectionsWaitingForHello_.insert(connection);
  auto pbPacketIn = std::make_shared<proto::Packet>();
  connection->read(
      *pbPacketIn,
      readPacketCallbackWrapper_(
          [pbPacketIn,
           transport{std::move(transport)},
           weakConnection{std::weak_ptr<transport::Connection>(connection)}](
              Listener& listener) mutable {
            std::shared_ptr<transport::Connection> connection =
                weakConnection.lock();
            TP_DCHECK(connection);
            listener.connectionsWaitingForHello_.erase(connection);
            listener.onConnectionHelloRead_(
                std::move(transport), std::move(connection), *pbPacketIn);
          }));
}

void Listener::armListener_(std::string transport) {
  TP_DCHECK(inLoop_());
  auto iter = listeners_.find(transport);
  if (iter == listeners_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  auto transportListener = iter->second;
  transportListener->accept(acceptCallbackWrapper_(
      [transport](
          Listener& listener,
          std::shared_ptr<transport::Connection> connection) {
        listener.onAccept_(transport, std::move(connection));
        listener.armListener_(transport);
      }));
}

void Listener::onConnectionHelloRead_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(inLoop_());
  if (pbPacketIn.has_spontaneous_connection()) {
    acceptCallback_.trigger(
        Error::kSuccess, std::move(transport), std::move(connection));
  } else if (pbPacketIn.has_requested_connection()) {
    const proto::RequestedConnection& pbRequestedConnection =
        pbPacketIn.requested_connection();
    uint64_t registrationId = pbRequestedConnection.registration_id();
    auto fn = std::move(connectionRequestRegistrations_.at(registrationId));
    connectionRequestRegistrations_.erase(registrationId);
    triggerConnectionRequestCallback_(
        std::move(fn),
        Error::kSuccess,
        std::move(transport),
        std::move(connection));
  } else {
    TP_LOG_ERROR() << "packet contained unknown content: "
                   << pbPacketIn.type_case();
  }
}

} // namespace tensorpipe
