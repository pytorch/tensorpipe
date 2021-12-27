/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/connection_impl.h>

#include <string.h>

#include <deque>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/ringbuffer_read_write_ops.h>
#include <tensorpipe/common/ringbuffer_role.h>
#include <tensorpipe/common/shm_ringbuffer.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/shm/context_impl.h>
#include <tensorpipe/transport/shm/reactor.h>
#include <tensorpipe/transport/shm/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace shm {

ConnectionImpl::ConnectionImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    Socket socket)
    : ConnectionImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      socket_(std::move(socket)) {}

ConnectionImpl::ConnectionImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string addr)
    : ConnectionImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      sockaddr_(Sockaddr::createAbstractUnixAddr(addr)) {}

void ConnectionImpl::initImplFromLoop() {
  context_->enroll(*this);

  Error error;
  // The connection either got a socket or an address, but not both.
  TP_DCHECK(socket_.hasValue() ^ sockaddr_.has_value());
  if (!socket_.hasValue()) {
    std::tie(error, socket_) = Socket::createForFamily(AF_UNIX);
    if (error) {
      setError(std::move(error));
      return;
    }
    error = socket_.connect(sockaddr_.value());
    if (error) {
      setError(std::move(error));
      return;
    }
  }
  // Ensure underlying control socket is non-blocking such that it
  // works well with event driven I/O.
  error = socket_.block(false);
  if (error) {
    setError(std::move(error));
    return;
  }

  // Create ringbuffer for inbox.
  std::tie(error, inboxHeaderSegment_, inboxDataSegment_, inboxRb_) =
      createShmRingBuffer<kNumRingbufferRoles>(kBufferSize);
  TP_THROW_ASSERT_IF(error)
      << "Couldn't allocate ringbuffer for connection inbox: " << error.what();

  // Register method to be called when our peer writes to our inbox.
  inboxReactorToken_ = context_->addReaction([this]() {
    TP_VLOG(9) << "Connection " << id_
               << " is reacting to the peer writing to the inbox";
    processReadOperationsFromLoop();
  });

  // Register method to be called when our peer reads from our outbox.
  outboxReactorToken_ = context_->addReaction([this]() {
    TP_VLOG(9) << "Connection " << id_
               << " is reacting to the peer reading from the outbox";
    processWriteOperationsFromLoop();
  });

  // We're sending file descriptors first, so wait for writability.
  state_ = SEND_FDS;
  context_->registerDescriptor(socket_.fd(), EPOLLOUT, shared_from_this());
}

void ConnectionImpl::readImplFromLoop(read_callback_fn fn) {
  readOperations_.emplace_back(std::move(fn));

  // If the inbox already contains some data, we may be able to process this
  // operation right away.
  processReadOperationsFromLoop();
}

void ConnectionImpl::readImplFromLoop(
    AbstractNopHolder& object,
    read_nop_callback_fn fn) {
  readOperations_.emplace_back(
      &object,
      [fn{std::move(fn)}](
          const Error& error, const void* /* unused */, size_t /* unused */) {
        fn(error);
      });

  // If the inbox already contains some data, we may be able to process this
  // operation right away.
  processReadOperationsFromLoop();
}

void ConnectionImpl::readImplFromLoop(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  readOperations_.emplace_back(ptr, length, std::move(fn));

  // If the inbox already contains some data, we may be able to process this
  // operation right away.
  processReadOperationsFromLoop();
}

void ConnectionImpl::writeImplFromLoop(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  writeOperations_.emplace_back(ptr, length, std::move(fn));

  // If the outbox has some free space, we may be able to process this operation
  // right away.
  processWriteOperationsFromLoop();
}

void ConnectionImpl::writeImplFromLoop(
    const AbstractNopHolder& object,
    write_callback_fn fn) {
  writeOperations_.emplace_back(&object, std::move(fn));

  // If the outbox has some free space, we may be able to process this operation
  // right away.
  processWriteOperationsFromLoop();
}

void ConnectionImpl::handleEventsFromLoop(int events) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_ << " is handling an event on its socket ("
             << EpollLoop::formatEpollEvents(events) << ")";

  // Handle only one of the events in the mask. Events on the control
  // file descriptor are rare enough for the cost of having epoll call
  // into this function multiple times to not matter. The benefit is
  // that every handler can close and unregister the control file
  // descriptor from the event loop, without worrying about the next
  // handler trying to do so as well.
  // In some cases the socket could be in a state where it's both in an error
  // state and readable/writable. If we checked for EPOLLIN or EPOLLOUT first
  // and then returned after handling them, we would keep doing so forever and
  // never reach the error handling. So we should keep the error check first.
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
  if (events & EPOLLIN) {
    handleEventInFromLoop();
    return;
  }
  if (events & EPOLLOUT) {
    handleEventOutFromLoop();
    return;
  }
  // Check for hangup last, as there could be cases where we get EPOLLHUP but
  // there's still data to be read from the socket, so we want to deal with that
  // before dealing with the hangup.
  if (events & EPOLLHUP) {
    setError(TP_CREATE_ERROR(EOFError));
    return;
  }
}

void ConnectionImpl::handleEventInFromLoop() {
  TP_DCHECK(context_->inLoop());
  if (state_ == RECV_FDS) {
    Fd reactorHeaderFd;
    Fd reactorDataFd;
    Fd outboxHeaderFd;
    Fd outboxDataFd;
    Reactor::TToken peerInboxReactorToken;
    Reactor::TToken peerOutboxReactorToken;

    // Receive the reactor token, reactor fds, and inbox fds.
    auto err = socket_.recvPayloadAndFds(
        peerInboxReactorToken,
        peerOutboxReactorToken,
        reactorHeaderFd,
        reactorDataFd,
        outboxHeaderFd,
        outboxDataFd);
    if (err) {
      setError(std::move(err));
      return;
    }

    // Load ringbuffer for outbox.
    std::tie(err, outboxHeaderSegment_, outboxDataSegment_, outboxRb_) =
        loadShmRingBuffer<kNumRingbufferRoles>(
            std::move(outboxHeaderFd), std::move(outboxDataFd));
    TP_THROW_ASSERT_IF(err)
        << "Couldn't access ringbuffer of connection outbox: " << err.what();

    // Initialize remote reactor trigger.
    peerReactorTrigger_.emplace(
        std::move(reactorHeaderFd), std::move(reactorDataFd));

    peerInboxReactorToken_ = peerInboxReactorToken;
    peerOutboxReactorToken_ = peerOutboxReactorToken;

    // The connection is usable now.
    state_ = ESTABLISHED;
    processWriteOperationsFromLoop();
    // Trigger read operations in case a pair of local read() and remote
    // write() happened before connection is established. Otherwise read()
    // callback would lose if it's the only read() request.
    processReadOperationsFromLoop();
    return;
  }

  if (state_ == ESTABLISHED) {
    // We don't expect to read anything on this socket once the
    // connection has been established. If we do, assume it's a
    // zero-byte read indicating EOF.
    setError(TP_CREATE_ERROR(EOFError));
    return;
  }

  TP_THROW_ASSERT() << "EPOLLIN event not handled in state " << state_;
}

void ConnectionImpl::handleEventOutFromLoop() {
  TP_DCHECK(context_->inLoop());
  if (state_ == SEND_FDS) {
    int reactorHeaderFd;
    int reactorDataFd;
    std::tie(reactorHeaderFd, reactorDataFd) = context_->reactorFds();

    // Send our reactor token, reactor fds, and inbox fds.
    auto err = socket_.sendPayloadAndFds(
        inboxReactorToken_.value(),
        outboxReactorToken_.value(),
        reactorHeaderFd,
        reactorDataFd,
        inboxHeaderSegment_.getFd(),
        inboxDataSegment_.getFd());
    if (err) {
      setError(std::move(err));
      return;
    }

    // Sent our fds. Wait for fds from peer.
    state_ = RECV_FDS;
    context_->registerDescriptor(socket_.fd(), EPOLLIN, shared_from_this());
    return;
  }

  TP_THROW_ASSERT() << "EPOLLOUT event not handled in state " << state_;
}

void ConnectionImpl::processReadOperationsFromLoop() {
  TP_DCHECK(context_->inLoop());

  // Process all read read operations that we can immediately serve, only
  // when connection is established.
  if (state_ != ESTABLISHED) {
    return;
  }
  // Serve read operations
  Consumer inboxConsumer(inboxRb_);
  while (!readOperations_.empty()) {
    RingbufferReadOperation& readOperation = readOperations_.front();
    if (readOperation.handleRead(inboxConsumer) > 0) {
      peerReactorTrigger_->run(peerOutboxReactorToken_.value());
    }
    if (readOperation.completed()) {
      readOperations_.pop_front();
    } else {
      break;
    }
  }
}

void ConnectionImpl::processWriteOperationsFromLoop() {
  TP_DCHECK(context_->inLoop());

  if (state_ != ESTABLISHED) {
    return;
  }

  Producer outboxProducer(outboxRb_);
  while (!writeOperations_.empty()) {
    RingbufferWriteOperation& writeOperation = writeOperations_.front();
    if (writeOperation.handleWrite(outboxProducer) > 0) {
      peerReactorTrigger_->run(peerInboxReactorToken_.value());
    }
    if (writeOperation.completed()) {
      writeOperations_.pop_front();
    } else {
      break;
    }
  }
}

void ConnectionImpl::handleErrorImpl() {
  for (auto& readOperation : readOperations_) {
    readOperation.handleError(error_);
  }
  readOperations_.clear();
  for (auto& writeOperation : writeOperations_) {
    writeOperation.handleError(error_);
  }
  writeOperations_.clear();
  if (inboxReactorToken_.has_value()) {
    context_->removeReaction(inboxReactorToken_.value());
    inboxReactorToken_.reset();
  }
  if (outboxReactorToken_.has_value()) {
    context_->removeReaction(outboxReactorToken_.value());
    outboxReactorToken_.reset();
  }
  if (socket_.hasValue()) {
    if (state_ > INITIALIZING) {
      context_->unregisterDescriptor(socket_.fd());
    }
    socket_.reset();
  }

  context_->unenroll(*this);
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
