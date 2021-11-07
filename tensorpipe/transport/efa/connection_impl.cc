/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/efa/connection_impl.h>

#include <string.h>

#include <deque>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/efa.h>
#include <tensorpipe/common/efa_read_write_ops.h>
#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/memory.h>
#include <tensorpipe/common/socket.h>
#include <tensorpipe/transport/efa/context_impl.h>
#include <tensorpipe/transport/efa/error.h>
#include <tensorpipe/transport/efa/reactor.h>
#include <tensorpipe/transport/efa/sockaddr.h>
#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {
namespace efa {

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
      sockaddr_(Sockaddr::createInetSockAddr(addr)) {}

void ConnectionImpl::initImplFromLoop() {
  context_->enroll(*this);
  Error error;
  // The connection either got a socket or an address, but not both.

  TP_DCHECK(socket_.hasValue() ^ sockaddr_.has_value());
  if (!socket_.hasValue()) {
    std::tie(error, socket_) =
        Socket::createForFamily(sockaddr_->addr()->sa_family);
    if (error) {
      setError(std::move(error));
      return;
    }
    error = socket_.reuseAddr(true);
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

  // We're sending address first, so wait for writability.
  state_ = SEND_ADDR;
  context_->registerDescriptor(socket_.fd(), EPOLLOUT, shared_from_this());
}

void ConnectionImpl::readImplFromLoop(read_callback_fn fn) {
  readOperations_.emplace_back(this, std::move(fn));

  processReadOperationsFromLoop();
}

void ConnectionImpl::readImplFromLoop(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  readOperations_.emplace_back(ptr, length, this, std::move(fn));

  // If the inbox already contains some data, we may be able to process this
  // operation right away.
  processReadOperationsFromLoop();
}

void ConnectionImpl::writeImplFromLoop(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  writeOperations_.emplace_back(ptr, length, this, std::move(fn));

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
  if (state_ == RECV_ADDR) {
    struct EfaAddress addr;
    // auto x = &addr.name;
    auto err = socket_.read(addr.name, sizeof(addr.name));
    // Crossing our fingers that the exchange information is small enough that
    // it can be read in a single chunk.
    if (err != sizeof(addr.name)) {
      setError(TP_CREATE_ERROR(ShortReadError, sizeof(addr.name), err));
      return;
    }

    peerAddr_ = context_->getReactor().addPeerAddr(addr);

    // The connection is usable now.
    state_ = ESTABLISHED;
    // Trigger read operations in case a pair of local read() and remote
    // write() happened before connection is established. Otherwise read()
    // callback would lose if it's the only read() request.
    processReadOperationsFromLoop();
    processWriteOperationsFromLoop();
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
  if (state_ == SEND_ADDR) {
    EfaAddress addr = context_->getReactor().getEfaAddress();
    auto err =
        socket_.write(reinterpret_cast<void*>(addr.name), sizeof(addr.name));
    // Crossing our fingers that the exchange information is small enough that
    // it can be written in a single chunk.
    if (err != sizeof(addr.name)) {
      setError(TP_CREATE_ERROR(ShortWriteError, sizeof(addr.name), err));
      return;
    }

    // Sent our address. Wait for address from peer.
    state_ = RECV_ADDR;
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

  for (int i = 0; i < readOperations_.size(); i++) {
    EFAReadOperation& readOperation = readOperations_[i];
    if (!readOperation.posted()) {
      // context_->getReactor().;
      context_->getReactor().postRecv(
          readOperation.getLengthPtr(),
          sizeof(size_t),
          kLength | recvIdx_,
          peerAddr_,
          0,
          &readOperation);
      readOperation.setWaitToCompleted();
      recvIdx_++;
    } else {
      // if the operation is posted, all operations back should be posted
      // we can skip more checks
      // break;
    }
  }
}

void ConnectionImpl::onWriteCompleted() {
  while (!writeOperations_.empty()) {
    EFAWriteOperation& writeOperation = writeOperations_.front();
    if (writeOperation.completed()) {
      writeOperation.callbackFromLoop(Error::kSuccess);
      writeOperations_.pop_front();
    } else {
      break;
    }
  }
}

void ConnectionImpl::onReadCompleted() {
  while (!readOperations_.empty()) {
    EFAReadOperation& readOperation = readOperations_.front();
    if (readOperation.completed()) {
      readOperation.callbackFromLoop(Error::kSuccess);
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

  for (int i = 0; i < writeOperations_.size(); i++) {
    EFAWriteOperation& writeOperation = writeOperations_[i];
    if (!writeOperation.posted()) {
      EFAWriteOperation::Buf* bufArray;
      size_t size;
      std::tie(bufArray, size) = writeOperation.getBufs();
      context_->getReactor().postSend(
          bufArray[0].base,
          bufArray[0].len,
          kLength | sendIdx_,
          peerAddr_,
          &writeOperation);
      if (size > 1) {
        context_->getReactor().postSend(
            bufArray[1].base,
            bufArray[1].len,
            kPayload | sendIdx_,
            peerAddr_,
            &writeOperation);
      }
      writeOperation.setWaitComplete();
      sendIdx_++;
    } else {
      // if the operation is posted, all operations back should be posted
      // we can skip more checks
      // break;
    }
  }
}

void ConnectionImpl::handleErrorImpl() {
  for (auto& readOperation : readOperations_) {
    readOperation.callbackFromLoop(error_);
  }
  readOperations_.clear();

  for (auto& writeOperation : writeOperations_) {
    writeOperation.callbackFromLoop(error_);
  }
  writeOperations_.clear();

  cleanup();

  if (socket_.hasValue()) {
    if (state_ > INITIALIZING) {
      context_->unregisterDescriptor(socket_.fd());
    }
    socket_.reset();
  }

  context_->unenroll(*this);
}

void ConnectionImpl::cleanup() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(8) << "Connection " << id_ << " is cleaning up";
  context_->getReactor().removePeerAddr(peerAddr_);
}

} // namespace efa
} // namespace transport
} // namespace tensorpipe
