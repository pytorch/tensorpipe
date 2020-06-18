/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/listener.h>

#include <deque>
#include <functional>
#include <mutex>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/socket.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Listener::Impl : public std::enable_shared_from_this<Listener::Impl>,
                       public EventHandler {
 public:
  // Create a listener that listens on the specified address.
  Impl(
      std::shared_ptr<Context::PrivateIface> context,
      address_t addr,
      std::string id);

  // Initialize member fields that need `shared_from_this`.
  void init();

  // Queue a callback to be called when a connection comes in.
  void accept(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addr() const;

  // Tell the listener what its identifier is.
  void setId(std::string id);

  // Shut down the connection and its resources.
  void close();

  // Implementation of EventHandler.
  void handleEventsFromLoop(int events) override;

 private:
  // Initialize member fields that need `shared_from_this`.
  void initFromLoop();

  // Queue a callback to be called when a connection comes in.
  void acceptFromLoop(accept_callback_fn fn);

  // Obtain the listener's address.
  std::string addrFromLoop() const;

  void setIdFromLoop_(std::string id);

  // Shut down the connection and its resources.
  void closeFromLoop();

  void setError_(Error error);

  // Deal with an error.
  void handleError();

  std::shared_ptr<Context::PrivateIface> context_;
  std::shared_ptr<Socket> socket_;
  Sockaddr sockaddr_;
  Error error_{Error::kSuccess};
  std::deque<accept_callback_fn> fns_;
  ClosingReceiver closingReceiver_;

  // A sequence number for the calls to accept.
  uint64_t nextConnectionBeingAccepted_{0};

  // A sequence number for the invocations of the callbacks of accept.
  uint64_t nextAcceptCallbackToCall_{0};

  // An identifier for the listener, composed of the identifier for the context,
  // combined with an increasing sequence number. It will be used as a prefix
  // for the identifiers of connections. All of them will only be used for
  // logging and debugging purposes.
  std::string id_;

  // Sequence numbers for the connections created by this listener, used to
  // create their identifiers based off this listener's identifier. They will
  // only be used for logging and debugging.
  std::atomic<uint64_t> connectionCounter_{0};
};

Listener::Impl::Impl(
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr,
    std::string id)
    : context_(std::move(context)),
      sockaddr_(Sockaddr::createAbstractUnixAddr(addr)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

void Listener::Impl::initFromLoop() {
  TP_DCHECK(context_->inLoopThread());

  closingReceiver_.activate(*this);

  Error error;
  TP_DCHECK(socket_ == nullptr);
  std::tie(error, socket_) = Socket::createForFamily(AF_UNIX);
  if (error) {
    setError_(std::move(error));
    return;
  }
  error = socket_->bind(sockaddr_);
  if (error) {
    setError_(std::move(error));
    return;
  }
  error = socket_->block(false);
  if (error) {
    setError_(std::move(error));
    return;
  }
  error = socket_->listen(128);
  if (error) {
    setError_(std::move(error));
    return;
  }
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::PrivateIface> context,
    address_t addr,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(addr),
          std::move(id))) {
  impl_->init();
}

void Listener::Impl::init() {
  context_->deferToLoop([impl{shared_from_this()}]() { impl->initFromLoop(); });
}

void Listener::Impl::closeFromLoop() {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(7) << "Listener " << id_ << " is closing";
  setError_(TP_CREATE_ERROR(ListenerClosedError));
}

void Listener::Impl::setError_(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void Listener::Impl::handleError() {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(8) << "Listener " << id_ << " is handling error " << error_.what();

  if (!fns_.empty()) {
    context_->unregisterDescriptor(socket_->fd());
  }
  socket_.reset();
  for (auto& fn : fns_) {
    fn(error_, std::shared_ptr<Connection>());
  }
  fns_.clear();
}

void Listener::close() {
  impl_->close();
}

void Listener::Impl::close() {
  context_->deferToLoop(
      [impl{shared_from_this()}]() { impl->closeFromLoop(); });
}

Listener::~Listener() {
  close();
}

void Listener::accept(accept_callback_fn fn) {
  impl_->accept(std::move(fn));
}

void Listener::Impl::accept(accept_callback_fn fn) {
  context_->deferToLoop(
      [impl{shared_from_this()}, fn{std::move(fn)}]() mutable {
        impl->acceptFromLoop(std::move(fn));
      });
}

void Listener::Impl::acceptFromLoop(accept_callback_fn fn) {
  TP_DCHECK(context_->inLoopThread());

  uint64_t sequenceNumber = nextConnectionBeingAccepted_++;
  TP_VLOG(7) << "Listener " << id_ << " received an accept request (#"
             << sequenceNumber << ")";

  fn = [this, sequenceNumber, fn{std::move(fn)}](
           const Error& error,
           std::shared_ptr<transport::Connection> connection) {
    TP_DCHECK_EQ(sequenceNumber, nextAcceptCallbackToCall_++);
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

  fns_.push_back(std::move(fn));

  // Only register if we go from 0 to 1 pending callbacks. In other cases we
  // already had a pending callback and thus we were already registered.
  if (fns_.size() == 1) {
    // Register with loop for readability events.
    context_->registerDescriptor(socket_->fd(), EPOLLIN, shared_from_this());
  }
}

address_t Listener::addr() const {
  return impl_->addr();
}

std::string Listener::Impl::addr() const {
  std::string addr;
  context_->runInLoop([this, &addr]() { addr = addrFromLoop(); });
  return addr;
}

address_t Listener::Impl::addrFromLoop() const {
  TP_DCHECK(context_->inLoopThread());
  return sockaddr_.str();
}

void Listener::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Listener::Impl::setId(std::string id) {
  context_->deferToLoop(
      [impl{shared_from_this()}, id{std::move(id)}]() mutable {
        impl->setIdFromLoop_(std::move(id));
      });
}

void Listener::Impl::setIdFromLoop_(std::string id) {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(7) << "Listener " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

void Listener::Impl::handleEventsFromLoop(int events) {
  TP_DCHECK(context_->inLoopThread());
  TP_VLOG(9) << "Listener " << id_ << " is handling an event on its socket ("
             << Loop::formatEpollEvents(events) << ")";

  if (events & EPOLLERR) {
    int error;
    socklen_t errorlen = sizeof(error);
    int rv = getsockopt(
        socket_->fd(),
        SOL_SOCKET,
        SO_ERROR,
        reinterpret_cast<void*>(&error),
        &errorlen);
    if (rv == -1) {
      setError_(TP_CREATE_ERROR(SystemError, "getsockopt", rv));
    } else {
      setError_(TP_CREATE_ERROR(SystemError, "async error on socket", error));
    }
    return;
  }
  if (events & EPOLLHUP) {
    setError_(TP_CREATE_ERROR(EOFError));
    return;
  }
  TP_ARG_CHECK_EQ(events, EPOLLIN);

  Error error;
  std::shared_ptr<Socket> socket;
  std::tie(error, socket) = socket_->accept();
  if (error) {
    setError_(std::move(error));
    return;
  }

  TP_DCHECK(!fns_.empty())
      << "when the callback is disarmed the listener's descriptor is supposed "
      << "to be unregistered";
  auto fn = std::move(fns_.front());
  fns_.pop_front();
  if (fns_.empty()) {
    context_->unregisterDescriptor(socket_->fd());
  }
  std::string connectionId = id_ + ".c" + std::to_string(connectionCounter_++);
  TP_VLOG(7) << "Listener " << id_ << " is opening connection " << connectionId;
  fn(Error::kSuccess,
     std::make_shared<Connection>(
         Connection::ConstructorToken(),
         context_,
         std::move(socket),
         std::move(connectionId)));
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
