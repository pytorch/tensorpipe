/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string.h>

#include <deque>
#include <memory>
#include <string>

#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/ibv.h>
#include <tensorpipe/common/memory.h>
#include <tensorpipe/common/nop.h>
#include <tensorpipe/common/ringbuffer.h>
#include <tensorpipe/common/ringbuffer_read_write_ops.h>
#include <tensorpipe/common/socket.h>
#include <tensorpipe/transport/connection_impl_boilerplate.h>
#include <tensorpipe/transport/ibv/reactor.h>
#include <tensorpipe/transport/ibv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

class ContextImpl;
class ListenerImpl;

class ConnectionImpl final : public ConnectionImplBoilerplate<
                                 ContextImpl,
                                 ListenerImpl,
                                 ConnectionImpl>,
                             public EpollLoop::EventHandler,
                             public IbvEventHandler {
  constexpr static size_t kBufferSize = 2 * 1024 * 1024;

  constexpr static int kNumOutboxRingbufferRoles = 3;
  using OutboxIbvAcker = RingBufferRole<kNumOutboxRingbufferRoles, 0>;
  using OutboxIbvWriter = RingBufferRole<kNumOutboxRingbufferRoles, 1>;
  using OutboxProducer = RingBufferRole<kNumOutboxRingbufferRoles, 2>;

  constexpr static int kNumInboxRingbufferRoles = 2;
  using InboxConsumer = RingBufferRole<kNumInboxRingbufferRoles, 0>;
  using InboxIbvRecver = RingBufferRole<kNumInboxRingbufferRoles, 1>;

  enum State {
    INITIALIZING = 1,
    SEND_ADDR,
    RECV_ADDR,
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

  // Implementation of IbvEventHandler.
  void onRemoteProducedData(uint32_t length) override;
  void onRemoteConsumedData(uint32_t length) override;
  void onWriteCompleted() override;
  void onAckCompleted() override;
  void onError(IbvLib::wc_status status, uint64_t wrId) override;

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
  // The only data that is expected on that socket is the address and other
  // setup information for the other side's queue pair and inbox.
  void handleEventInFromLoop();

  // Handle events of type EPOLLOUT on the UNIX domain socket.
  //
  // Once the socket is writable we send the address and other setup information
  // for this side's queue pair and inbox.
  void handleEventOutFromLoop();

  State state_{INITIALIZING};
  Socket socket_;
  optional<Sockaddr> sockaddr_;

  IbvQueuePair qp_;

  // Inbox.
  // Initialize header during construction because it isn't assignable.
  RingBufferHeader<kNumInboxRingbufferRoles> inboxHeader_{kBufferSize};
  // Use mmapped memory so it's page-aligned (and, one day, to use huge pages).
  MmappedPtr inboxBuf_;
  RingBuffer<kNumInboxRingbufferRoles> inboxRb_;
  IbvMemoryRegion inboxMr_;

  // Outbox.
  // Initialize header during construction because it isn't assignable.
  RingBufferHeader<kNumOutboxRingbufferRoles> outboxHeader_{kBufferSize};
  // Use mmapped memory so it's page-aligned (and, one day, to use huge pages).
  MmappedPtr outboxBuf_;
  RingBuffer<kNumOutboxRingbufferRoles> outboxRb_;
  IbvMemoryRegion outboxMr_;

  // Peer inbox key, pointer and head.
  uint32_t peerInboxKey_{0};
  uint64_t peerInboxPtr_{0};
  uint64_t peerInboxHead_{0};

  // The connection performs two types of send requests: writing to the remote
  // inbox, or acknowledging a write into its own inbox. These send operations
  // could be delayed and stalled by the reactor as only a limited number of
  // work requests can be outstanding at the same time globally. Thus we keep
  // count of how many we have pending to make sure they have all completed or
  // flushed when we close, and that none is stuck in the pipeline.
  uint32_t numWritesInFlight_{0};
  uint32_t numAcksInFlight_{0};

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

  void tryCleanup();
  void cleanup();
};

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
