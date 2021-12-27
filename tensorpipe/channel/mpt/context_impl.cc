/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/mpt/context_impl.h>

#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/mpt/channel_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

namespace {

std::string generateDomainDescriptor(
    const std::vector<std::shared_ptr<transport::Context>>& contexts) {
  // FIXME Escape the contexts' domain descriptors in case they contain a colon?
  // Or put them all in a nop object, that'll do the escaping for us.
  // But is it okay to compare nop objects by equality bitwise?
  std::ostringstream ss;
  ss << contexts.size();
  for (const auto& context : contexts) {
    ss << ":" << context->domainDescriptor();
  }
  return ss.str();
}

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create(
    std::vector<std::shared_ptr<transport::Context>> contexts,
    std::vector<std::shared_ptr<transport::Listener>> listeners) {
  for (const auto& context : contexts) {
    if (!context->isViable()) {
      return nullptr;
    }
  }

  std::unordered_map<Device, std::string> deviceDescriptors = {
      {Device{kCpuDeviceType, 0}, generateDomainDescriptor(contexts)}};

  return std::make_shared<ContextImpl>(
      std::move(contexts), std::move(listeners), std::move(deviceDescriptors));
}

ContextImpl::ContextImpl(
    std::vector<std::shared_ptr<transport::Context>> contexts,
    std::vector<std::shared_ptr<transport::Listener>> listeners,
    std::unordered_map<Device, std::string> deviceDescriptors)
    : ContextImplBoilerplate<ContextImpl, ChannelImpl>(
          std::move(deviceDescriptors)),
      contexts_(std::move(contexts)),
      listeners_(std::move(listeners)) {
  TP_THROW_ASSERT_IF(contexts_.size() != listeners_.size());
  numLanes_ = contexts_.size();

  addresses_.reserve(numLanes_);
  for (const auto& listener : listeners_) {
    addresses_.emplace_back(listener->addr());
  }
}

void ContextImpl::initImplFromLoop() {
  for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
    acceptLane(laneIdx);
  }
}

std::shared_ptr<Channel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint endpoint) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(std::move(connections[0]), endpoint, numLanes_);
}

const std::vector<std::string>& ContextImpl::addresses() const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return addresses_;
}

uint64_t ContextImpl::registerConnectionRequest(
    uint64_t laneIdx,
    connection_request_callback_fn fn) {
  TP_DCHECK(loop_.inLoop());

  uint64_t registrationId = nextConnectionRequestRegistrationId_++;

  TP_VLOG(4) << "Channel context " << id_
             << " received a connection request registration (#"
             << registrationId << ") on lane " << laneIdx;

  fn = [this, registrationId, fn{std::move(fn)}](
           const Error& error,
           std::shared_ptr<transport::Connection> connection) {
    TP_VLOG(4) << "Channel context " << id_
               << " calling a connection request registration callback (#"
               << registrationId << ")";
    fn(error, std::move(connection));
    TP_VLOG(4) << "Channel context " << id_
               << " done calling a connection request registration callback (#"
               << registrationId << ")";
  };

  if (error_) {
    fn(error_, std::shared_ptr<transport::Connection>());
  } else {
    connectionRequestRegistrations_.emplace(registrationId, std::move(fn));
  }

  return registrationId;
}

void ContextImpl::unregisterConnectionRequest(uint64_t registrationId) {
  TP_DCHECK(loop_.inLoop());

  TP_VLOG(4) << "Channel context " << id_
             << " received a connection request de-registration (#"
             << registrationId << ")";

  connectionRequestRegistrations_.erase(registrationId);
}

std::shared_ptr<transport::Connection> ContextImpl::connect(
    uint64_t laneIdx,
    std::string address) {
  TP_VLOG(4) << "Channel context " << id_ << " opening connection on lane "
             << laneIdx;
  return contexts_[laneIdx]->connect(std::move(address));
}

void ContextImpl::acceptLane(uint64_t laneIdx) {
  TP_DCHECK(loop_.inLoop());

  TP_VLOG(6) << "Channel context " << id_ << " accepting connection on lane "
             << laneIdx;
  listeners_[laneIdx]->accept(
      callbackWrapper_([laneIdx](
                           ContextImpl& impl,
                           std::shared_ptr<transport::Connection> connection) {
        TP_VLOG(6) << "Channel context " << impl.id_
                   << " done accepting connection on lane " << laneIdx;
        if (impl.error_) {
          return;
        }
        impl.onAcceptOfLane(std::move(connection));
        impl.acceptLane(laneIdx);
      }));
}

void ContextImpl::onAcceptOfLane(
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(loop_.inLoop());

  // Keep it alive until we figure out what to do with it.
  connectionsWaitingForHello_.insert(connection);
  auto npHolderIn = std::make_shared<NopHolder<Packet>>();
  TP_VLOG(6) << "Channel context " << id_
             << " reading nop object (client hello)";
  connection->read(
      *npHolderIn,
      callbackWrapper_([npHolderIn, connection](ContextImpl& impl) mutable {
        TP_VLOG(6) << "Channel context " << impl.id_
                   << " done reading nop object (client hello)";
        if (impl.error_) {
          return;
        }
        impl.connectionsWaitingForHello_.erase(connection);
        impl.onReadClientHelloOnLane(
            std::move(connection), npHolderIn->getObject());
      }));
}

void ContextImpl::onReadClientHelloOnLane(
    std::shared_ptr<transport::Connection> connection,
    const Packet& nopPacketIn) {
  TP_DCHECK(loop_.inLoop());
  TP_DCHECK_EQ(nopPacketIn.index(), nopPacketIn.index_of<ClientHello>());

  const ClientHello& nopClientHello = *nopPacketIn.get<ClientHello>();
  uint64_t registrationId = nopClientHello.registrationId;
  auto iter = connectionRequestRegistrations_.find(registrationId);
  // The connection request may have already been deregistered, for example
  // because the channel may have been closed.
  if (iter != connectionRequestRegistrations_.end()) {
    auto fn = std::move(iter->second);
    connectionRequestRegistrations_.erase(iter);
    fn(Error::kSuccess, std::move(connection));
  }
}

void ContextImpl::handleErrorImpl() {
  for (auto& iter : connectionRequestRegistrations_) {
    connection_request_callback_fn fn = std::move(iter.second);
    fn(error_, std::shared_ptr<transport::Connection>());
  }
  connectionRequestRegistrations_.clear();

  for (const auto& connection : connectionsWaitingForHello_) {
    connection->close();
  }
  connectionsWaitingForHello_.clear();

  for (auto& listener : listeners_) {
    listener->close();
  }
  for (auto& context : contexts_) {
    context->close();
  }
}

void ContextImpl::setIdImpl() {
  for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
    contexts_[laneIdx]->setId(id_ + ".ctx_" + std::to_string(laneIdx));
    listeners_[laneIdx]->setId(
        id_ + ".ctx_" + std::to_string(laneIdx) + ".l_" +
        std::to_string(laneIdx));
  }
}

void ContextImpl::joinImpl() {
  for (auto& context : contexts_) {
    context->join();
  }
}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
