/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/listener_impl.h>

#include <deque>
#include <functional>
#include <mutex>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/ibv/connection_impl.h>
#include <tensorpipe/transport/ibv/context_impl.h>
#include <tensorpipe/transport/ibv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

ListenerImpl::ListenerImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string addr)
    : ListenerImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      sockaddr_(Sockaddr::createInetSockAddr(addr)) {}

void ListenerImpl::initImplFromLoop() {
  context_->enroll(*this);

  Error error;
  TP_DCHECK(!socket_.hasValue());
  std::tie(error, socket_) =
      Socket::createForFamily(sockaddr_.addr()->sa_family);
  if (error) {
    setError(std::move(error));
    return;
  }
  error = socket_.reuseAddr(true);
  if (error) {
    setError(std::move(error));
    return;
  }
  error = socket_.bind(sockaddr_);
  if (error) {
    setError(std::move(error));
    return;
  }
  error = socket_.block(false);
  if (error) {
    setError(std::move(error));
    return;
  }
  error = socket_.listen(128);
  if (error) {
    setError(std::move(error));
    return;
  }
}

void ListenerImpl::handleErrorImpl() {
  if (!fns_.empty()) {
    context_->unregisterDescriptor(socket_.fd());
  }
  socket_.reset();
  for (auto& fn : fns_) {
    fn(error_, std::shared_ptr<Connection>());
  }
  fns_.clear();

  context_->unenroll(*this);
}

void ListenerImpl::acceptImplFromLoop(accept_callback_fn fn) {
  fns_.push_back(std::move(fn));

  // Only register if we go from 0 to 1 pending callbacks. In other cases we
  // already had a pending callback and thus we were already registered.
  if (fns_.size() == 1) {
    // Register with loop for readability events.
    context_->registerDescriptor(socket_.fd(), EPOLLIN, shared_from_this());
  }
}

std::string ListenerImpl::addrImplFromLoop() const {
  struct sockaddr_storage ss;
  struct sockaddr* addr = reinterpret_cast<struct sockaddr*>(&ss);
  socklen_t addrlen = sizeof(ss);
  int rv = getsockname(socket_.fd(), addr, &addrlen);
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  return Sockaddr(addr, addrlen).str();
}

void ListenerImpl::handleEventsFromLoop(int events) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Listener " << id_ << " is handling an event on its socket ("
             << EpollLoop::formatEpollEvents(events) << ")";

  if (events & EPOLLERR) {
    int error;
    socklen_t errorlen = sizeof(error);
    int rv = getsockopt(
        socket_.fd(),
        SOL_SOCKET,
        SO_ERROR,
        reinterpret_cast<void*>(&error),
        &errorlen);
    if (rv == -1) {
      setError(TP_CREATE_ERROR(SystemError, "getsockopt", rv));
    } else {
      setError(TP_CREATE_ERROR(SystemError, "async error on socket", error));
    }
    return;
  }
  if (events & EPOLLHUP) {
    setError(TP_CREATE_ERROR(EOFError));
    return;
  }
  TP_ARG_CHECK_EQ(events, EPOLLIN);

  Error error;
  Socket socket;
  std::tie(error, socket) = socket_.accept();
  if (error) {
    setError(std::move(error));
    return;
  }

  TP_DCHECK(!fns_.empty())
      << "when the callback is disarmed the listener's descriptor is supposed "
      << "to be unregistered";
  auto fn = std::move(fns_.front());
  fns_.pop_front();
  if (fns_.empty()) {
    context_->unregisterDescriptor(socket_.fd());
  }
  fn(Error::kSuccess, createAndInitConnection(std::move(socket)));
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
