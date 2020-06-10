/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/listener.h>

#include <deque>
#include <unordered_set>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/proto/core.pb.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {

class Listener::Impl : public Listener::PrivateIface,
                       public std::enable_shared_from_this<Listener::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::PrivateIface>,
      std::string id,
      const std::vector<std::string>& urls);

  // Called by the listener's constructor.
  void init();

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
  OnDemandLoop loop_;

  void acceptFromLoop_(accept_callback_fn);

  void closeFromLoop_();

  Error error_{Error::kSuccess};

  std::shared_ptr<Context::PrivateIface> context_;

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
  std::map<std::string, transport::address_t> addresses_;

  // A sequence number for the calls to accept.
  uint64_t nextPipeBeingAccepted_{0};

  // A sequence number for the invocations of the callbacks of accept.
  uint64_t nextAcceptCallbackToCall_{0};

  RearmableCallback<const Error&, std::shared_ptr<Pipe>> acceptCallback_;

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

  void initFromLoop_();

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

  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, this->loop_};

  //
  // Error handling
  //

  void setError_(Error error);

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

  template <typename T>
  friend class LazyCallbackWrapper;
};

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    std::string id,
    const std::vector<std::string>& urls)
    : impl_(std::make_shared<Listener::Impl>(
          std::move(context),
          std::move(id),
          urls)) {
  impl_->init();
}

Listener::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    std::string id,
    const std::vector<std::string>& urls)
    : context_(std::move(context)),
      id_(std::move(id)),
      closingReceiver_(context_, context_->getClosingEmitter()) {
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

void Listener::Impl::init() {
  loop_.deferToLoop([this]() { initFromLoop_(); });
}

void Listener::Impl::initFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  closingReceiver_.activate(*this);
  for (const auto& listener : listeners_) {
    armListener_(listener.first);
  }
}

void Listener::close() {
  impl_->close();
}

void Listener::Impl::close() {
  loop_.deferToLoop([this]() { closeFromLoop_(); });
}

void Listener::Impl::closeFromLoop_() {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(1) << "Listener " << id_ << " is closing";
  setError_(TP_CREATE_ERROR(ListenerClosedError));
}

Listener::~Listener() {
  close();
}

//
// Entry points for user code
//

void Listener::accept(accept_callback_fn fn) {
  impl_->accept(std::move(fn));
}

void Listener::Impl::accept(accept_callback_fn fn) {
  loop_.deferToLoop(
      [this, fn{std::move(fn)}]() mutable { acceptFromLoop_(std::move(fn)); });
}

void Listener::Impl::acceptFromLoop_(accept_callback_fn fn) {
  TP_DCHECK(loop_.inLoop());

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

const std::map<std::string, std::string>& Listener::addresses() const {
  return impl_->addresses();
}

const std::map<std::string, std::string>& Listener::Impl::addresses() const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return addresses_;
}

const std::string& Listener::address(const std::string& transport) const {
  return impl_->address(transport);
}

const std::string& Listener::Impl::address(const std::string& transport) const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  const auto it = addresses_.find(transport);
  TP_THROW_ASSERT_IF(it == addresses_.end())
      << ": transport '" << transport << "' not in use by this listener.";
  return it->second;
}

std::string Listener::url(const std::string& transport) const {
  return impl_->url(transport);
}

std::string Listener::Impl::url(const std::string& transport) const {
  // As this is an immutable member (after it has been initialized in
  // the constructor), we'll access it without deferring to the loop.
  return transport + "://" + address(transport);
}

//
// Entry points for internal code
//

uint64_t Listener::Impl::registerConnectionRequest(
    connection_request_callback_fn fn) {
  // We cannot return a value if we defer the function. Thus we obtain an ID
  // now (and this is why the next ID is an atomic), return it, and defer the
  // rest of the processing.
  // FIXME Avoid this hack by doing like we did with the channels' recv: have
  // this accept a callback that is called with the registration ID.
  uint64_t registrationId = nextConnectionRequestRegistrationId_++;
  loop_.deferToLoop([this, registrationId, fn{std::move(fn)}]() mutable {
    registerConnectionRequestFromLoop_(registrationId, std::move(fn));
  });
  return registrationId;
}

void Listener::Impl::registerConnectionRequestFromLoop_(
    uint64_t registrationId,
    connection_request_callback_fn fn) {
  TP_DCHECK(loop_.inLoop());

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
}

void Listener::Impl::unregisterConnectionRequest(uint64_t registrationId) {
  loop_.deferToLoop([this, registrationId]() {
    unregisterConnectionRequestFromLoop_(registrationId);
  });
}

void Listener::Impl::unregisterConnectionRequestFromLoop_(
    uint64_t registrationId) {
  TP_DCHECK(loop_.inLoop());
  TP_VLOG(1) << "Listener " << id_
             << " received a connection request de-registration (#"
             << registrationId << ")";
  connectionRequestRegistrations_.erase(registrationId);
}

//
// Error handling
//

void Listener::Impl::setError_(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError_();
}

void Listener::Impl::handleError_() {
  TP_DCHECK(loop_.inLoop());
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
  connectionsWaitingForHello_.clear();
}

//
// Everything else
//

void Listener::Impl::onAccept_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection) {
  TP_DCHECK(loop_.inLoop());
  // Keep it alive until we figure out what to do with it.
  connectionsWaitingForHello_.insert(connection);
  auto pbPacketIn = std::make_shared<proto::Packet>();
  TP_VLOG(3) << "Listener " << id_
             << " is reading proto (spontaneous or requested connection)";
  connection->read(
      *pbPacketIn,
      lazyCallbackWrapper_([pbPacketIn,
                            transport{std::move(transport)},
                            weakConnection{std::weak_ptr<transport::Connection>(
                                connection)}](Impl& impl) mutable {
        TP_VLOG(3)
            << "Listener " << impl.id_
            << " done reading proto (spontaneous or requested connection)";
        std::shared_ptr<transport::Connection> connection =
            weakConnection.lock();
        TP_DCHECK(connection);
        impl.connectionsWaitingForHello_.erase(connection);
        impl.onConnectionHelloRead_(
            std::move(transport), std::move(connection), *pbPacketIn);
      }));
}

void Listener::Impl::armListener_(std::string transport) {
  TP_DCHECK(loop_.inLoop());
  auto iter = listeners_.find(transport);
  if (iter == listeners_.end()) {
    TP_THROW_EINVAL() << "unsupported transport " << transport;
  }
  auto transportListener = iter->second;
  TP_VLOG(3) << "Listener " << id_ << " is accepting connection on transport "
             << transport;
  transportListener->accept(lazyCallbackWrapper_(
      [transport](
          Impl& impl, std::shared_ptr<transport::Connection> connection) {
        TP_VLOG(3) << "Listener " << impl.id_
                   << " done accepting connection on transport " << transport;
        impl.onAccept_(transport, std::move(connection));
        impl.armListener_(transport);
      }));
}

void Listener::Impl::onConnectionHelloRead_(
    std::string transport,
    std::shared_ptr<transport::Connection> connection,
    const proto::Packet& pbPacketIn) {
  TP_DCHECK(loop_.inLoop());
  if (pbPacketIn.has_spontaneous_connection()) {
    const proto::SpontaneousConnection& pbSpontaneousConnection =
        pbPacketIn.spontaneous_connection();
    TP_VLOG(3) << "Listener " << id_ << " got spontaneous connection";
    std::string pipeId = id_ + ".p" + std::to_string(pipeCounter_++);
    TP_VLOG(1) << "Listener " << id_ << " is opening pipe " << pipeId;
    const std::string& remoteContextName =
        pbSpontaneousConnection.context_name();
    if (remoteContextName != "") {
      std::string aliasPipeId = id_ + "_from_" + remoteContextName;
      TP_VLOG(1) << "Pipe " << pipeId << " aliased as " << aliasPipeId;
      pipeId = std::move(aliasPipeId);
    }
    auto pipe = std::make_shared<Pipe>(
        Pipe::ConstructorToken(),
        context_,
        std::static_pointer_cast<PrivateIface>(shared_from_this()),
        std::move(pipeId),
        remoteContextName,
        std::move(transport),
        std::move(connection));
    acceptCallback_.trigger(Error::kSuccess, std::move(pipe));
  } else if (pbPacketIn.has_requested_connection()) {
    const proto::RequestedConnection& pbRequestedConnection =
        pbPacketIn.requested_connection();
    uint64_t registrationId = pbRequestedConnection.registration_id();
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
                   << pbPacketIn.type_case();
  }
}

} // namespace tensorpipe
