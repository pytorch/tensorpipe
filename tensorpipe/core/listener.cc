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
  std::unique_lock<std::mutex> lock(mutex_);
  for (const auto& listener : listeners_) {
    armListener_(listener.first, lock);
  }
}

//
// Entry points for user code
//

void Listener::accept(accept_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (error_) {
    triggerAcceptCallback_(
        std::move(fn),
        error_,
        std::string(),
        std::shared_ptr<transport::Connection>(),
        lock);
    return;
  }

  acceptCallback_.arm(
      runIfAlive(
          *this,
          std::function<void(
              Listener&,
              const Error&,
              std::string,
              std::shared_ptr<transport::Connection>,
              TLock)>([fn{std::move(fn)}](
                          Listener& listener,
                          const Error& error,
                          std::string transport,
                          std::shared_ptr<transport::Connection> connection,
                          TLock lock) mutable {
            listener.triggerAcceptCallback_(
                std::move(fn),
                error,
                std::move(transport),
                std::move(connection),
                lock);
          })),
      lock);
}

const std::map<std::string, std::string>& Listener::addresses() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return addresses_;
}

const std::string& Listener::address(const std::string& transport) const {
  std::unique_lock<std::mutex> lock(mutex_);
  const auto it = addresses_.find(transport);
  TP_THROW_ASSERT_IF(it == addresses_.end())
      << ": transport '" << transport << "' not in use by this listener.";
  return it->second;
}

std::string Listener::url(const std::string& transport) const {
  // std::unique_lock<std::mutex> lock(mutex_);
  return transport + "://" + address(transport);
}

//
// Entry points for internal code
//

uint64_t Listener::registerConnectionRequest_(
    connection_request_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);

  uint64_t registrationId = nextConnectionRequestRegistrationId_++;
  if (error_) {
    triggerConnectionRequestCallback_(
        std::move(fn),
        error_,
        std::string(),
        std::shared_ptr<transport::Connection>(),
        lock);
  } else {
    connectionRequestRegistrations_.emplace(registrationId, std::move(fn));
  }
  return registrationId;
}

void Listener::unregisterConnectionRequest_(uint64_t registrationId) {
  std::unique_lock<std::mutex> lock(mutex_);
  connectionRequestRegistrations_.erase(registrationId);
}

//
// Helpers to schedule our callbacks into user code
//

void Listener::triggerAcceptCallback_(
    accept_callback_fn fn,
    const Error& error,
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  lock.unlock();
  // Create the pipe here, without holding the lock, as otherwise TSAN would
  // report a false positive lock order inversion.
  std::shared_ptr<Pipe> pipe;
  if (!error) {
    pipe = std::make_shared<Pipe>(
        Pipe::ConstructorToken(),
        context_,
        shared_from_this(),
        std::move(transport),
        std::move(connection));
    pipe->start_();
  }
  fn(error, std::move(pipe));
  lock.lock();
}

void Listener::triggerConnectionRequestCallback_(
    connection_request_callback_fn fn,
    const Error& error,
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  lock.unlock();
  fn(error, std::move(transport), std::move(connection));
  lock.lock();
}

//
// Error handling
//

void Listener::handleError_(TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);

  acceptCallback_.triggerAll(
      [&]() {
        return std::make_tuple(
            error_, std::string(), std::shared_ptr<transport::Connection>());
      },
      lock);
  for (auto& iter : connectionRequestRegistrations_) {
    connection_request_callback_fn fn = std::move(iter.second);
    triggerConnectionRequestCallback_(
        std::move(fn),
        error_,
        std::string(),
        std::shared_ptr<transport::Connection>(),
        lock);
  }
  connectionRequestRegistrations_.clear();
}

//
// Everything else
//

void Listener::onAccept_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  // Keep it alive until we figure out what to do with it.
  connectionsWaitingForHello_.insert(connection);
  auto pbPacketIn = std::make_shared<proto::Packet>();
  connection->read(
      *pbPacketIn,
      readPacketCallbackWrapper_(
          [pbPacketIn,
           transport{std::move(transport)},
           weakConnection{std::weak_ptr<transport::Connection>(connection)}](
              Listener& listener, TLock lock) mutable {
            std::shared_ptr<transport::Connection> connection =
                weakConnection.lock();
            TP_DCHECK(connection);
            listener.connectionsWaitingForHello_.erase(connection);
            listener.onConnectionHelloRead_(
                std::move(transport), std::move(connection), *pbPacketIn, lock);
          }));
}

void Listener::armListener_(std::string transport, TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  auto iter = listeners_.find(transport);
  if (iter == listeners_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  auto transportListener = iter->second;
  transportListener->accept(acceptCallbackWrapper_(
      [transport](
          Listener& listener,
          std::shared_ptr<transport::Connection> connection,
          TLock lock) {
        listener.onAccept_(transport, std::move(connection), lock);
        listener.armListener_(transport, lock);
      }));
}

void Listener::onConnectionHelloRead_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    const proto::Packet& pbPacketIn,
    TLock lock) {
  TP_DCHECK(lock.owns_lock() && lock.mutex() == &mutex_);
  if (pbPacketIn.has_spontaneous_connection()) {
    acceptCallback_.trigger(
        Error::kSuccess, std::move(transport), std::move(connection), lock);
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
        std::move(connection),
        lock);
  } else {
    TP_LOG_ERROR() << "packet contained unknown content: "
                   << pbPacketIn.type_case();
  }
}

} // namespace tensorpipe
