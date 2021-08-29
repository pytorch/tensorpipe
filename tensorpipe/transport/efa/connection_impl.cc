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
#include <tensorpipe/common/efa_read_write_ops.h>
#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/fabric.h>
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

namespace {

// When the connection gets closed, to avoid leaks, it needs to "reclaim" all
// the work requests that it had posted, by waiting for their completion. They
// may however complete with error, which makes it harder to identify and
// distinguish them from failing incoming requests because, in principle, we
// cannot access the opcode field of a failed work completion. Therefore, we
// assign a special ID to those types of requests, to match them later on.
constexpr uint64_t kWriteRequestId = 1;
constexpr uint64_t kAckRequestId = 2;

// The data that each queue pair endpoint needs to send to the other endpoint in
// order to set up the queue pair itself. This data is transferred over a TCP
// connection.
// struct Exchange {
//   efaSetupInformation setupInfo;
//   uint64_t memoryRegionPtr;
//   uint32_t memoryRegionKey;
// };

} // namespace

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
  // Register methods to be called when our peer writes to our inbox and reads
  // from our outbox.
  // context_->getReactor().registerQp(qp_->qp_num, shared_from_this());

  // We're sending address first, so wait for writability.
  state_ = SEND_ADDR;
  context_->registerDescriptor(socket_.fd(), EPOLLOUT, shared_from_this());
}

void ConnectionImpl::readImplFromLoop(read_callback_fn fn) {
  readOperations_.emplace_back(std::move(fn));

  processReadOperationsFromLoop();
}

// void ConnectionImpl::readImplFromLoop(
//     AbstractNopHolder& object,
//     read_nop_callback_fn fn) {
//   readOperations_.emplace_back(
//       &object,
//       [fn{std::move(fn)}](
//           const Error& error, const void* /* unused */, size_t /* unused */)
//           {
//         fn(error);
//       });

//   // If the inbox already contains some data, we may be able to process this
//   // operation right away.
//   processReadOperationsFromLoop();
// }

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
    struct FabricAddr addr;

    auto err = socket_.read(&addr.name, 64);
    // Crossing our fingers that the exchange information is small enough that
    // it can be read in a single chunk.
    if (err != 64) {
      setError(TP_CREATE_ERROR(ShortReadError, 64, err));
      return;
    }

    peer_addr = endpoint->AddPeerAddr(&addr);

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
  if (state_ == SEND_ADDR) {
    FabricAddr addr = endpoint->fabric_ctx->addr;

    auto err = socket_.write(reinterpret_cast<void*>(&addr.name), 64);
    // Crossing our fingers that the exchange information is small enough that
    // it can be written in a single chunk.
    if (err != 64) {
      setError(TP_CREATE_ERROR(ShortWriteError, 64, err));
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

  // pop out finished event at front
  while (!readOperations_.empty()) {
    EFAReadOperation& readOperation = readOperations_.front();
    if (readOperation.completeFromLoop()) {
      readOperation.callbackFromLoop(Error::kSuccess);
      readOperations_.pop_front();
    } else {
      break;
    }
  }

  // Serve read operations
  // while (!readOperations_.empty()) {
  //   EFAReadOperation& readOperation = readOperations_.front();
  //   context_->getReactor().postRecv(
  //       &readOperation.readLength_,
  //       sizeof(size_t),
  //       kLength,
  //       peer_addr,
  //       0xffffffff, // ignore lower bits for msg index
  //       &readOperation);
  // }

  for (int i = 0; i < readOperations_.size(); i++) {
    EFAReadOperation& readOperation = readOperations_[i];
    if (readOperation.mode_ == EFAReadOperation::Mode::READ_LENGTH) {
      // context_->getReactor().;
      context_->getReactor().postRecv(
          &readOperation.readLength_,
          sizeof(size_t),
          kLength,
          peer_addr,
          0xffffffff, // ignore lower bits for msg index
          &readOperation);
      readOperation.mode_ = EFAReadOperation::Mode::READ_PAYLOAD;
    } else {
      // if the operation is not READ_LENGTH, all operations back are all not
      // READ_LENGTH, we can skip more checks
      break;
    }
  }
}

void ConnectionImpl::processWriteOperationsFromLoop() {
  TP_DCHECK(context_->inLoop());

  if (state_ != ESTABLISHED) {
    return;
  }

  while (!writeOperations_.empty()) {
    EFAWriteOperation& writeOperation = writeOperations_.front();
    if (writeOperation.mode_ == EFAWriteOperation::Mode::COMPLETE) {
      writeOperations_.pop_front();
    } else {
      break;
    }
  }

  for (int i = 0; i < writeOperations_.size(); i++) {
    EFAWriteOperation& writeOperation = writeOperations_[i];
    if (writeOperation.mode_ == EFAWriteOperation::Mode::WRITE_LENGTH) {
      EFAWriteOperation::Buf* buf_array;
      size_t size;
      std::tie(buf_array, size) = writeOperation.getBufs();
      // auto size_buf = std::get<0>(writeOperation.getBufs());
      // auto payload_buf = std::get<1>(writeOperation.getBufs());
      context_->getReactor().postSend(
          buf_array[0].base,
          buf_array[0].len,
          kLength | sendIdx,
          peer_addr,
          &writeOperation);
      if (size > 1) {
        context_->getReactor().postSend(
            buf_array[1].base,
            buf_array[1].len,
            kPayload | sendIdx,
            peer_addr,
            &writeOperation);
      }
      sendIdx++;
      writeOperation.mode_ = EFAWriteOperation::Mode::WAIT_TO_COMPLETE;
    } else {
      // if the operation is not WAIT_TO_SEND, all operations back are all not
      // WAIT_TO_SEND, we can skip more checks
      break;
    }
  }
}

// void ConnectionImpl::onError(efaLib::wc_status status, uint64_t wrId) {
//   TP_DCHECK(context_->inLoop());
//   // setError(TP_CREATE_ERROR(
//   //     efaError,
//   context_->getReactor().getefaLib().wc_status_str(status)));
//   // if (wrId == kWriteRequestId) {
//   //   onWriteCompleted();
//   // } else if (wrId == kAckRequestId) {
//   //   onAckCompleted();
//   // }
// }

void ConnectionImpl::handleErrorImpl() {
  for (auto& readOperation : readOperations_) {
    readOperation.callbackFromLoop(error_);
  }
  readOperations_.clear();

  for (auto& writeOperation : writeOperations_) {
    writeOperation.callbackFromLoop(error_);
  }
  writeOperations_.clear();

  tryCleanup();

  if (socket_.hasValue()) {
    if (state_ > INITIALIZING) {
      context_->unregisterDescriptor(socket_.fd());
    }
    socket_.reset();
  }

  context_->unenroll(*this);
}

void ConnectionImpl::tryCleanup() {
  TP_DCHECK(context_->inLoop());
  // Setting the queue pair to an error state will cause all its work requests
  // (both those that had started being served, and those that hadn't; including
  // those from a shared receive queue) to be flushed. We need to wait for the
  // completion events of all those requests to be retrieved from the completion
  // queue before we can destroy the queue pair. We can do so by deferring the
  // destruction to the loop, since the reactor will only proceed to invoke
  // deferred functions once it doesn't have any completion events to handle.
  // However the RDMA writes and the sends may be queued up inside the reactor
  // and thus may not have even been scheduled yet, so we explicitly wait for
  // them to complete.
  // if (error_) {
  //   if (numWritesInFlight_ == 0 && numAcksInFlight_ == 0) {
  //     TP_VLOG(8) << "Connection " << id_ << " is ready to clean up";
  //     context_->deferToLoop([impl{shared_from_this()}]() { impl->cleanup();
  //     });
  //   } else {
  //     TP_VLOG(9) << "Connection " << id_
  //                << " cannot proceed to cleanup because it has "
  //                << numWritesInFlight_ << " pending RDMA write requests and "
  //                << numAcksInFlight_ << " pending send requests on QP "
  //                << qp_->qp_num;
  //   }
  // }
}

void ConnectionImpl::cleanup() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(8) << "Connection " << id_ << " is cleaning up";

  // context_->getReactor().unregisterQp(qp_->qp_num);

  // qp_.reset();
  // inboxMr_.reset();
  // inboxBuf_.reset();
  // outboxMr_.reset();
  // outboxBuf_.reset();
}

} // namespace efa
} // namespace transport
} // namespace tensorpipe
