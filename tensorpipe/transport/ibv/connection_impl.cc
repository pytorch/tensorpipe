/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/connection_impl.h>

#include <string.h>

#include <deque>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/ibv.h>
#include <tensorpipe/common/memory.h>
#include <tensorpipe/common/ringbuffer_read_write_ops.h>
#include <tensorpipe/common/ringbuffer_role.h>
#include <tensorpipe/common/socket.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/ibv/constants.h>
#include <tensorpipe/transport/ibv/context_impl.h>
#include <tensorpipe/transport/ibv/error.h>
#include <tensorpipe/transport/ibv/reactor.h>
#include <tensorpipe/transport/ibv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

namespace {

// The data that each queue pair endpoint needs to send to the other endpoint in
// order to set up the queue pair itself. This data is transferred over a TCP
// connection.
struct Exchange {
  IbvSetupInformation setupInfo;
  uint64_t memoryRegionPtr;
  uint32_t memoryRegionKey;
};

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

  // Create ringbuffer for inbox.
  std::tie(error, inboxBuf_) = MmappedPtr::create(
      kBufferSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1);
  TP_THROW_ASSERT_IF(error)
      << "Couldn't allocate ringbuffer for connection inbox: " << error.what();
  inboxRb_ =
      RingBuffer<kNumInboxRingbufferRoles>(&inboxHeader_, inboxBuf_.ptr());
  inboxMr_ = createIbvMemoryRegion(
      context_->getReactor().getIbvLib(),
      context_->getReactor().getIbvPd(),
      inboxBuf_.ptr(),
      kBufferSize,
      IbvLib::ACCESS_LOCAL_WRITE | IbvLib::ACCESS_REMOTE_WRITE);

  // Create ringbuffer for outbox.
  std::tie(error, outboxBuf_) = MmappedPtr::create(
      kBufferSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1);
  TP_THROW_ASSERT_IF(error)
      << "Couldn't allocate ringbuffer for connection outbox: " << error.what();
  outboxRb_ =
      RingBuffer<kNumOutboxRingbufferRoles>(&outboxHeader_, outboxBuf_.ptr());
  outboxMr_ = createIbvMemoryRegion(
      context_->getReactor().getIbvLib(),
      context_->getReactor().getIbvPd(),
      outboxBuf_.ptr(),
      kBufferSize,
      0);

  // Create and init queue pair.
  {
    IbvLib::qp_init_attr initAttr;
    std::memset(&initAttr, 0, sizeof(initAttr));
    initAttr.qp_type = IbvLib::QPT_RC;
    initAttr.send_cq = context_->getReactor().getIbvCq().get();
    initAttr.recv_cq = context_->getReactor().getIbvCq().get();
    initAttr.cap.max_send_wr = kSendQueueSize;
    initAttr.cap.max_send_sge = 1;
    initAttr.srq = context_->getReactor().getIbvSrq().get();
    initAttr.sq_sig_all = 1;
    qp_ = createIbvQueuePair(
        context_->getReactor().getIbvLib(),
        context_->getReactor().getIbvPd(),
        initAttr);
  }
  transitionIbvQueuePairToInit(
      context_->getReactor().getIbvLib(),
      qp_,
      context_->getReactor().getIbvAddress());

  // Register methods to be called when our peer writes to our inbox and reads
  // from our outbox.
  context_->getReactor().registerQp(qp_->qp_num, shared_from_this());

  // We're sending address first, so wait for writability.
  state_ = SEND_ADDR;
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
  if (state_ == RECV_ADDR) {
    struct Exchange ex;

    auto err = socket_.read(&ex, sizeof(ex));
    // Crossing our fingers that the exchange information is small enough that
    // it can be read in a single chunk.
    if (err != sizeof(ex)) {
      setError(TP_CREATE_ERROR(ShortReadError, sizeof(ex), err));
      return;
    }

    transitionIbvQueuePairToReadyToReceive(
        context_->getReactor().getIbvLib(),
        qp_,
        context_->getReactor().getIbvAddress(),
        ex.setupInfo);
    transitionIbvQueuePairToReadyToSend(
        context_->getReactor().getIbvLib(), qp_);

    peerInboxKey_ = ex.memoryRegionKey;
    peerInboxPtr_ = ex.memoryRegionPtr;

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
    Exchange ex;
    ex.setupInfo =
        makeIbvSetupInformation(context_->getReactor().getIbvAddress(), qp_);
    ex.memoryRegionPtr = reinterpret_cast<uint64_t>(inboxBuf_.ptr());
    ex.memoryRegionKey = inboxMr_->rkey;

    auto err = socket_.write(reinterpret_cast<void*>(&ex), sizeof(ex));
    // Crossing our fingers that the exchange information is small enough that
    // it can be written in a single chunk.
    if (err != sizeof(ex)) {
      setError(TP_CREATE_ERROR(ShortWriteError, sizeof(ex), err));
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
  // Serve read operations
  InboxConsumer inboxConsumer(inboxRb_);
  while (!readOperations_.empty()) {
    RingbufferReadOperation& readOperation = readOperations_.front();
    ssize_t len = readOperation.handleRead(inboxConsumer);
    if (len > 0) {
      Reactor::AckInfo info;
      info.length = len;

      TP_VLOG(9) << "Connection " << id_
                 << " is posting a send request (acknowledging " << info.length
                 << " bytes) on QP " << qp_->qp_num;
      context_->getReactor().postAck(qp_, info);
      numAcksInFlight_++;
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

  OutboxProducer outboxProducer(outboxRb_);
  while (!writeOperations_.empty()) {
    RingbufferWriteOperation& writeOperation = writeOperations_.front();
    ssize_t len = writeOperation.handleWrite(outboxProducer);
    if (len > 0) {
      ssize_t ret;
      OutboxIbvWriter outboxConsumer(outboxRb_);

      ret = outboxConsumer.startTx();
      TP_THROW_SYSTEM_IF(ret < 0, -ret);

      ssize_t numBuffers;
      std::array<OutboxIbvWriter::Buffer, 2> buffers;

      std::tie(numBuffers, buffers) =
          outboxConsumer.accessContiguousInTx</*AllowPartial=*/false>(len);
      TP_THROW_SYSTEM_IF(numBuffers < 0, -numBuffers);

      for (int bufferIdx = 0; bufferIdx < numBuffers; bufferIdx++) {
        Reactor::WriteInfo info;
        info.addr = buffers[bufferIdx].ptr;
        info.length = buffers[bufferIdx].len;
        info.lkey = outboxMr_->lkey;

        uint64_t peerInboxOffset = peerInboxHead_ & (kBufferSize - 1);
        peerInboxHead_ += buffers[bufferIdx].len;

        info.remoteAddr = peerInboxPtr_ + peerInboxOffset;
        info.rkey = peerInboxKey_;

        TP_VLOG(9) << "Connection " << id_
                   << " is posting a RDMA write request (transmitting "
                   << info.length << " bytes) on QP " << qp_->qp_num;
        context_->getReactor().postWrite(qp_, info);
        numWritesInFlight_++;
      }

      ret = outboxConsumer.commitTx();
      TP_THROW_SYSTEM_IF(ret < 0, -ret);
    }
    if (writeOperation.completed()) {
      writeOperations_.pop_front();
    } else {
      break;
    }
  }
}

void ConnectionImpl::onRemoteProducedData(uint32_t length) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_ << " was signalled that " << length
             << " bytes were written to its inbox on QP " << qp_->qp_num;

  ssize_t ret;
  InboxIbvRecver inboxProducer(inboxRb_);

  ret = inboxProducer.startTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  ret = inboxProducer.incMarkerInTx(length);
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  ret = inboxProducer.commitTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  processReadOperationsFromLoop();
}

void ConnectionImpl::onRemoteConsumedData(uint32_t length) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_ << " was signalled that " << length
             << " bytes were read from its outbox on QP " << qp_->qp_num;
  ssize_t ret;
  OutboxIbvAcker outboxConsumer(outboxRb_);

  ret = outboxConsumer.startTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  ret = outboxConsumer.incMarkerInTx(length);
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  ret = outboxConsumer.commitTx();
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  processWriteOperationsFromLoop();
}

void ConnectionImpl::onWriteCompleted() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_
             << " done posting a RDMA write request on QP " << qp_->qp_num;
  numWritesInFlight_--;
  tryCleanup();
}

void ConnectionImpl::onAckCompleted() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(9) << "Connection " << id_ << " done posting a send request on QP "
             << qp_->qp_num;
  numAcksInFlight_--;
  tryCleanup();
}

void ConnectionImpl::onError(IbvLib::wc_status status, uint64_t wrId) {
  TP_DCHECK(context_->inLoop());
  setError(TP_CREATE_ERROR(
      IbvError, context_->getReactor().getIbvLib().wc_status_str(status)));
  if (wrId == kWriteRequestId) {
    onWriteCompleted();
  } else if (wrId == kAckRequestId) {
    onAckCompleted();
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

  transitionIbvQueuePairToError(context_->getReactor().getIbvLib(), qp_);

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
  if (error_) {
    if (numWritesInFlight_ == 0 && numAcksInFlight_ == 0) {
      TP_VLOG(8) << "Connection " << id_ << " is ready to clean up";
      context_->deferToLoop([impl{shared_from_this()}]() { impl->cleanup(); });
    } else {
      TP_VLOG(9) << "Connection " << id_
                 << " cannot proceed to cleanup because it has "
                 << numWritesInFlight_ << " pending RDMA write requests and "
                 << numAcksInFlight_ << " pending send requests on QP "
                 << qp_->qp_num;
    }
  }
}

void ConnectionImpl::cleanup() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(8) << "Connection " << id_ << " is cleaning up";

  context_->getReactor().unregisterQp(qp_->qp_num);

  qp_.reset();
  inboxMr_.reset();
  inboxBuf_.reset();
  outboxMr_.reset();
  outboxBuf_.reset();
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
