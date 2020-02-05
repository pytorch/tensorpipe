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
#include <tensorpipe/proto/all.pb.h>

namespace tensorpipe {

std::shared_ptr<Listener> Listener::create(
    std::shared_ptr<Context> context,
    const std::vector<std::string>& urls) {
  std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
      transportListeners;
  for (const auto& url : urls) {
    std::string transport;
    std::string address;
    std::tie(transport, address) = splitSchemeOfURL(url);
    transportListeners.emplace(
        transport,
        context->getContextForTransport_(transport)->listen(address));
  }
  auto listener = std::make_shared<Listener>(
      ConstructorToken(), std::move(context), std::move(transportListeners));
  listener->start_();
  return listener;
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
        listeners)
    : context_(std::move(context)), listeners_(std::move(listeners)) {
  for (const auto& it : listeners_) {
    addresses_[it.first] = it.second->addr();
  }
}

void Listener::start_() {
  for (const auto& listener : listeners_) {
    armListener_(listener.first);
  }
}

void Listener::accept(accept_callback_fn fn) {
  acceptCallback_.arm(runIfAlive(
      *this,
      std::function<void(Listener&, const Error&, std::shared_ptr<Pipe>)>(
          [fn{std::move(fn)}](
              Listener& listener,
              const Error& error,
              std::shared_ptr<Pipe> pipe) {
            listener.context_->callCallback_(
                [fn{std::move(fn)}, error, pipe{std::move(pipe)}]() {
                  fn(error, std::move(pipe));
                });
          })));
}

const std::map<std::string, std::string>& Listener::addresses() const {
  return addresses_;
}

const std::string& Listener::address(const std::string& transport) const {
  const auto it = addresses_.find(transport);
  TP_THROW_ASSERT_IF(it == addresses_.end())
      << ": transport '" << transport << "' not in use by this listener.";
  return it->second;
}

std::string Listener::url(const std::string& transport) const {
  return transport + "://" + address(transport);
}

void Listener::onAccept_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  // Keep it alive until we figure out what to do with it.
  connectionsWaitingForHello_.insert(connection);
  connection->read(runIfAlive(
      *this,
      std::function<void(
          Listener&, const transport::Error&, const void*, size_t)>(
          [transport{std::move(transport)},
           weakConnection{std::weak_ptr<transport::Connection>(connection)}](
              Listener& listener,
              const transport::Error& /* unused */,
              const void* ptr,
              size_t len) mutable {
            // TODO Implement proper error handling in Listener.
            std::shared_ptr<transport::Connection> connection =
                weakConnection.lock();
            TP_DCHECK(connection);
            listener.connectionsWaitingForHello_.erase(connection);
            listener.onConnectionHelloRead_(
                std::move(transport), std::move(connection), ptr, len);
          })));
}

void Listener::armListener_(std::string transport) {
  auto iter = listeners_.find(transport);
  if (iter == listeners_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  auto transportListener = iter->second;
  transportListener->accept(runIfAlive(
      *this,
      std::function<void(
          Listener&,
          const transport::Error&,
          std::shared_ptr<transport::Connection>)>(
          [transport](
              Listener& listener,
              const transport::Error& /* unused */,
              std::shared_ptr<transport::Connection> connection) {
            // TODO Implement proper error handling in Listener.
            listener.onAccept_(transport, std::move(connection));
            listener.armListener_(transport);
          })));
}

void Listener::onConnectionHelloRead_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    const void* ptr,
    size_t len) {
  proto::Packet pbPacketIn;
  {
    bool success = pbPacketIn.ParseFromArray(ptr, len);
    TP_DCHECK(success) << "couldn't parse packet";
  }
  if (pbPacketIn.has_spontaneous_connection()) {
    std::shared_ptr<Pipe> pipe = std::make_shared<Pipe>(
        Pipe::ConstructorToken(),
        context_,
        shared_from_this(),
        std::move(transport),
        std::move(connection));
    pipe->start_();
    acceptCallback_.trigger(Error::kSuccess, std::move(pipe));
  } else if (pbPacketIn.has_requested_connection()) {
    const proto::RequestedConnection& pbRequestedConnection =
        pbPacketIn.requested_connection();
    uint64_t registrationId = pbRequestedConnection.registration_id();
    auto fn = std::move(connectionRequestRegistrations_.at(registrationId));
    connectionRequestRegistrations_.erase(registrationId);
    fn(std::move(transport), std::move(connection));
  } else {
    TP_LOG_ERROR() << "packet contained unknown content: "
                   << pbPacketIn.type_case();
  }
}

uint64_t Listener::registerConnectionRequest_(
    std::function<void(std::string, std::shared_ptr<transport::Connection>)>
        fn) {
  uint64_t registrationId = nextConnectionRequestRegistrationId_++;
  connectionRequestRegistrations_.emplace(registrationId, std::move(fn));
  return registrationId;
}

void Listener::unregisterConnectionRequest_(uint64_t registrationId) {
  connectionRequestRegistrations_.erase(registrationId);
}

} // namespace tensorpipe
