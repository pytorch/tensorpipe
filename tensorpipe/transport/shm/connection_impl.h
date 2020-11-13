/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <memory>
#include <string>

#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/nop.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/ringbuffer_read_write_ops.h>
#include <tensorpipe/common/socket.h>
#include <tensorpipe/transport/connection_impl_boilerplate.h>
#include <tensorpipe/transport/shm/reactor.h>
#include <tensorpipe/transport/shm/sockaddr.h>
#include <tensorpipe/util/ringbuffer/ringbuffer.h>
#include <tensorpipe/util/shm/segment.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

constexpr auto kBufferSize = 2 * 1024 * 1024;

} // namespace

class ContextImpl;
class ListenerImpl;

class ConnectionImpl final : public ConnectionImplBoilerplate<
                                 ContextImpl,
                                 ListenerImpl,
                                 ConnectionImpl>,
                             public EpollLoop::EventHandler {
  enum State {
    INITIALIZING = 1,
    SEND_FDS,
    RECV_FDS,
    ESTABLISHED,
  };

 public:
  // Create a connection that is already connected (e.g. from a listener).
  ConnectionImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      Socket socket);

  // Create a connection that connects to the specified address.
  ConnectionImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::string addr);

  // Implementation of EventHandler.
  void handleEventsFromLoop(int events) override;

 protected:
  // Implement the entry points called by ConnectionImplBoilerplate.
  void initImplFromLoop() override;
  void readImplFromLoop(read_callback_fn fn) override;
  void readImplFromLoop(AbstractNopHolder& object, read_nop_callback_fn fn)
      override;
  void readImplFromLoop(void* ptr, size_t length, read_callback_fn fn) override;
  void writeImplFromLoop(const void* ptr, size_t length, write_callback_fn fn)
      override;
  void writeImplFromLoop(const AbstractNopHolder& object, write_callback_fn fn)
      override;
  void handleErrorImpl() override;

 private:
  // Handle events of type EPOLLIN on the UNIX domain socket.
  //
  // The only data that is expected on that socket is the file descriptors for
  // the other side's inbox (which is this side's outbox) and its reactor, plus
  // the reactor tokens to trigger the other side to read or write.
  void handleEventInFromLoop();

  // Handle events of type EPOLLOUT on the UNIX domain socket.
  //
  // Once the socket is writable we send the file descriptors for this side's
  // inbox (which the other side's outbox) and our reactor, plus the reactor
  // tokens to trigger this connection to read or write.
  void handleEventOutFromLoop();

  State state_{INITIALIZING};
  Socket socket_;
  optional<Sockaddr> sockaddr_;

  // Inbox.
  util::shm::Segment inboxHeaderSegment_;
  util::shm::Segment inboxDataSegment_;
  util::ringbuffer::RingBuffer inboxRb_;
  optional<Reactor::TToken> inboxReactorToken_;

  // Outbox.
  util::shm::Segment outboxHeaderSegment_;
  util::shm::Segment outboxDataSegment_;
  util::ringbuffer::RingBuffer outboxRb_;
  optional<Reactor::TToken> outboxReactorToken_;

  // Peer trigger/tokens.
  optional<Reactor::Trigger> peerReactorTrigger_;
  optional<Reactor::TToken> peerInboxReactorToken_;
  optional<Reactor::TToken> peerOutboxReactorToken_;

  // Pending read operations.
  std::deque<RingbufferReadOperation> readOperations_;

  // Pending write operations.
  std::deque<RingbufferWriteOperation> writeOperations_;

  // Process pending read operations if in an operational state.
  //
  // This may be triggered by the other side of the connection (by pushing this
  // side's inbox token to the reactor) when it has written some new data to its
  // outbox (which is this side's inbox). It is also called by this connection
  // when it moves into an established state or when a new read operation is
  // queued, in case data was already available before this connection was ready
  // to consume it.
  void processReadOperationsFromLoop();

  // Process pending write operations if in an operational state.
  //
  // This may be triggered by the other side of the connection (by pushing this
  // side's outbox token to the reactor) when it has read some data from its
  // inbox (which is this side's outbox). This is important when some of this
  // side's writes couldn't complete because the outbox was full, and thus they
  // needed to wait for some of its data to be read. This method is also called
  // by this connection when it moves into an established state, in case some
  // writes were queued before the connection was ready to process them, or when
  // a new write operation is queued.
  void processWriteOperationsFromLoop();
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
