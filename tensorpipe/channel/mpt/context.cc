/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/mpt/context.h>

#include <algorithm>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/channel/mpt/channel.h>
#include <tensorpipe/channel/mpt/context_impl.h>
#include <tensorpipe/channel/mpt/nop_types.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace channel {
namespace mpt {

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  Impl(
      std::vector<std::shared_ptr<transport::Context>> contexts,
      std::vector<std::shared_ptr<transport::Listener>> listeners);

  // Called by the context's constructor.
  void init();

  const std::string& domainDescriptor() const;

  std::shared_ptr<channel::CpuChannel> createChannel(
      std::shared_ptr<transport::Connection> connection,
      Endpoint endpoint);

  ClosingEmitter& getClosingEmitter() override;

  const std::vector<std::string>& addresses() const override;

  uint64_t registerConnectionRequest(
      uint64_t laneIdx,
      connection_request_callback_fn fn) override;

  void unregisterConnectionRequest(uint64_t registrationId) override;

  std::shared_ptr<transport::Connection> connect(
      uint64_t laneIdx,
      std::string address) override;

  void setId(std::string id);

  void close();

  void join();

  ~Impl() override = default;

 private:
  void initFromLoop();

  void closeFromLoop();

  void setIdFromLoop(std::string id);

  void registerConnectionRequestFromLoop(
      uint64_t laneIdx,
      uint64_t registrationId,
      connection_request_callback_fn fn);

  void unregisterConnectionRequestFromLoop(uint64_t registrationId);

  void acceptLane(uint64_t laneIdx);
  void onAcceptOfLane(std::shared_ptr<transport::Connection> connection);
  void onReadClientHelloOnLane(
      std::shared_ptr<transport::Connection> connection,
      const Packet& nopPacketIn);

  void setError(Error error);

  void handleError();

  std::vector<std::shared_ptr<transport::Context>> contexts_;
  std::vector<std::shared_ptr<transport::Listener>> listeners_;

  std::string domainDescriptor_;
  std::atomic<bool> joined_{false};
  uint64_t numLanes_{0};
  std::vector<std::string> addresses_;

  // This is atomic because it may be accessed from outside the loop.
  std::atomic<uint64_t> nextConnectionRequestRegistrationId_{0};

  // Needed to keep them alive.
  std::unordered_set<std::shared_ptr<transport::Connection>>
      connectionsWaitingForHello_;

  std::unordered_map<uint64_t, connection_request_callback_fn>
      connectionRequestRegistrations_;

  // An identifier for the context, composed of the identifier for the context,
  // combined with the channel's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  // Sequence numbers for the channels created by this context, used to create
  // their identifiers based off this context's identifier. They will only be
  // used for logging and debugging.
  std::atomic<uint64_t> channelCounter_{0};

  OnDemandDeferredExecutor loop_;
  Error error_{Error::kSuccess};
  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, this->loop_};
  EagerCallbackWrapper<Impl> eagerCallbackWrapper_{*this, this->loop_};
  ClosingEmitter closingEmitter_;

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::LazyCallbackWrapper;
  template <typename T>
  friend class tensorpipe::EagerCallbackWrapper;
};

Context::Context(
    std::vector<std::shared_ptr<transport::Context>> contexts,
    std::vector<std::shared_ptr<transport::Listener>> listeners)
    : impl_(std::make_shared<Impl>(std::move(contexts), std::move(listeners))) {
  impl_->init();
}

Context::Impl::Impl(
    std::vector<std::shared_ptr<transport::Context>> contexts,
    std::vector<std::shared_ptr<transport::Listener>> listeners)
    : contexts_(std::move(contexts)), listeners_(std::move(listeners)) {
  TP_THROW_ASSERT_IF(contexts_.size() != listeners_.size());
  numLanes_ = contexts_.size();
  // FIXME Escape the contexts' domain descriptors in case they contain a colon?
  // Or put them all in a nop object, that'll do the escaping for us.
  // But is it okay to compare nop objects by equality bitwise?
  std::ostringstream ss;
  ss << contexts_.size();
  for (const auto& context : contexts_) {
    ss << ":" << context->domainDescriptor();
  }
  domainDescriptor_ = ss.str();

  addresses_.reserve(numLanes_);
  for (const auto& listener : listeners_) {
    addresses_.emplace_back(listener->addr());
  }
}

void Context::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop(); });
}

void Context::Impl::initFromLoop() {
  TP_DCHECK(loop_.inLoop());

  for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
    acceptLane(laneIdx);
  }
}

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
}

std::shared_ptr<channel::CpuChannel> Context::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint endpoint) {
  return impl_->createChannel(std::move(connection), endpoint);
}

std::shared_ptr<channel::CpuChannel> Context::Impl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint endpoint) {
  std::string channelId = id_ + ".c" + std::to_string(channelCounter_++);
  TP_VLOG(4) << "Channel context " << id_ << " is opening channel "
             << channelId;
  return std::make_shared<Channel>(
      Channel::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(connection),
      endpoint,
      numLanes_,
      std::move(channelId));
}

const std::vector<std::string>& Context::Impl::addresses() const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return addresses_;
}

uint64_t Context::Impl::registerConnectionRequest(
    uint64_t laneIdx,
    connection_request_callback_fn fn) {
  // We cannot return a value if we defer the function. Thus we obtain an ID
  // now (and this is why the next ID is an atomic), return it, and defer the
  // rest of the processing.
  // FIXME Avoid this hack by doing like we did with the channels' recv: have
  // this accept a callback that is called with the registration ID.
  uint64_t registrationId = nextConnectionRequestRegistrationId_++;
  loop_.deferToLoop(
      [this, laneIdx, registrationId, fn{std::move(fn)}]() mutable {
        registerConnectionRequestFromLoop(
            laneIdx, registrationId, std::move(fn));
      });
  return registrationId;
}

void Context::Impl::registerConnectionRequestFromLoop(
    uint64_t laneIdx,
    uint64_t registrationId,
    connection_request_callback_fn fn) {
  TP_DCHECK(loop_.inLoop());

  TP_VLOG(4) << "Channel context " << id_
             << " received a connection request registration (#"
             << registrationId << ") on lane " << laneIdx;

  if (error_) {
    TP_VLOG(4) << "Channel context " << id_
               << " calling a connection request registration callback (#"
               << registrationId << ")";
    fn(error_, std::shared_ptr<transport::Connection>());
    TP_VLOG(4) << "Channel context " << id_
               << " done calling a connection request registration callback (#"
               << registrationId << ")";
  } else {
    connectionRequestRegistrations_.emplace(registrationId, std::move(fn));
  }
}

void Context::Impl::unregisterConnectionRequest(uint64_t registrationId) {
  loop_.deferToLoop([this, registrationId]() {
    unregisterConnectionRequestFromLoop(registrationId);
  });
}

void Context::Impl::unregisterConnectionRequestFromLoop(
    uint64_t registrationId) {
  TP_DCHECK(loop_.inLoop());

  TP_VLOG(4) << "Channel context " << id_
             << " received a connection request de-registration (#"
             << registrationId << ")";

  connectionRequestRegistrations_.erase(registrationId);
}

std::shared_ptr<transport::Connection> Context::Impl::connect(
    uint64_t laneIdx,
    std::string address) {
  TP_VLOG(4) << "Channel context " << id_ << " opening connection on lane "
             << laneIdx;
  return contexts_[laneIdx]->connect(std::move(address));
}

void Context::Impl::acceptLane(uint64_t laneIdx) {
  TP_DCHECK(loop_.inLoop());

  TP_VLOG(6) << "Channel context " << id_ << " accepting connection on lane "
             << laneIdx;
  listeners_[laneIdx]->accept(lazyCallbackWrapper_(
      [laneIdx](Impl& impl, std::shared_ptr<transport::Connection> connection) {
        TP_VLOG(6) << "Channel context " << impl.id_
                   << " done accepting connection on lane " << laneIdx;
        impl.onAcceptOfLane(std::move(connection));
        impl.acceptLane(laneIdx);
      }));
}

void Context::Impl::onAcceptOfLane(
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(loop_.inLoop());

  // Keep it alive until we figure out what to do with it.
  connectionsWaitingForHello_.insert(connection);
  auto npHolderIn = std::make_shared<NopHolder<Packet>>();
  TP_VLOG(6) << "Channel context " << id_
             << " reading nop object (client hello)";
  connection->read(
      *npHolderIn,
      lazyCallbackWrapper_([npHolderIn,
                            weakConnection{std::weak_ptr<transport::Connection>(
                                connection)}](Impl& impl) mutable {
        TP_VLOG(6) << "Channel context " << impl.id_
                   << " done reading nop object (client hello)";
        std::shared_ptr<transport::Connection> connection =
            weakConnection.lock();
        TP_DCHECK(connection);
        impl.connectionsWaitingForHello_.erase(connection);
        impl.onReadClientHelloOnLane(
            std::move(connection), npHolderIn->getObject());
      }));
}

void Context::Impl::onReadClientHelloOnLane(
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

void Context::Impl::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void Context::Impl::handleError() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(5) << "Channel context " << id_ << " handling error "
             << error_.what();

  closingEmitter_.close();

  for (auto& iter : connectionRequestRegistrations_) {
    connection_request_callback_fn fn = std::move(iter.second);
    fn(error_, std::shared_ptr<transport::Connection>());
  }
  connectionRequestRegistrations_.clear();

  connectionsWaitingForHello_.clear();
  for (auto& listener : listeners_) {
    listener->close();
  }
  for (auto& context : contexts_) {
    context->close();
  }
}

void Context::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Context::Impl::setId(std::string id) {
  loop_.deferToLoop(
      [this, id{std::move(id)}]() mutable { setIdFromLoop(std::move(id)); });
}

void Context::Impl::setIdFromLoop(std::string id) {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(4) << "Channel context " << id_ << " was renamed to " << id;
  id_ = std::move(id);
  for (uint64_t laneIdx = 0; laneIdx < numLanes_; ++laneIdx) {
    contexts_[laneIdx]->setId(id_ + ".ctx_" + std::to_string(laneIdx));
    listeners_[laneIdx]->setId(
        id_ + ".ctx_" + std::to_string(laneIdx) + ".l_" +
        std::to_string(laneIdx));
  }
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  loop_.deferToLoop([this]() { closeFromLoop(); });
}

void Context::Impl::closeFromLoop() {
  TP_DCHECK(loop_.inLoop());

  TP_VLOG(4) << "Channel context " << id_ << " is closing";

  setError(TP_CREATE_ERROR(ContextClosedError));

  TP_VLOG(4) << "Channel context " << id_ << " done closing";
}

void Context::join() {
  impl_->join();
}

void Context::Impl::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(4) << "Channel context " << id_ << " is joining";

    for (auto& context : contexts_) {
      context->join();
    }

    TP_VLOG(4) << "Channel context " << id_ << " done joining";
  }
}

Context::~Context() {
  join();
}

} // namespace mpt
} // namespace channel
} // namespace tensorpipe
