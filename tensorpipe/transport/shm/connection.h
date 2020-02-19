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
#include <mutex>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/reactor.h>
#include <tensorpipe/transport/shm/socket.h>
#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>
#include <tensorpipe/util/ringbuffer/shm.h>

#include <google/protobuf/io/zero_copy_stream.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Connection final : public transport::Connection,
                         public std::enable_shared_from_this<Connection>,
                         public EventHandler {
  // Extra data stored in ringbuffer header.
  struct RingBufferExtraData {
    // Nothing yet.
  };

  using TRingBuffer = util::ringbuffer::RingBuffer<RingBufferExtraData>;
  using TProducer = util::ringbuffer::Producer<RingBufferExtraData>;
  using TConsumer = util::ringbuffer::Consumer<RingBufferExtraData>;

  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  static constexpr auto kDefaultSize = 2 * 1024 * 1024;

  enum State {
    INITIALIZING = 1,
    SEND_FDS,
    RECV_FDS,
    ESTABLISHED,
    DESTROYING,
  };

 public:
  static std::shared_ptr<Connection> create(
      std::shared_ptr<Loop> loop,
      std::shared_ptr<Socket> socket);

  Connection(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<Socket> socket);

  ~Connection() override;

  // Kickstart connection state machine. Must be called outside
  // constructor because it calls `shared_from_this()`.
  void start();

  // Implementation of transport::Connection.
  void read(read_callback_fn fn) override;

  // Implementation of transport::Connection.
  void read(void* ptr, size_t length, read_callback_fn fn) override;

  // Implementation of transport::Connection
  void write(const void* ptr, size_t length, write_callback_fn fn) override;

  // Implementation of transport::Connection
  void write(const google::protobuf::MessageLite& message, write_callback_fn fn)
      override;

  // Implementation of EventHandler.
  void handleEvents(int events) override;

  // Handle events of type EPOLLIN.
  void handleEventIn(std::unique_lock<std::mutex> lock);

  // Handle events of type EPOLLOUT.
  void handleEventOut(std::unique_lock<std::mutex> lock);

  // Handle events of type EPOLLERR.
  void handleEventErr(std::unique_lock<std::mutex> lock);

  // Handle events of type EPOLLHUP.
  void handleEventHup(std::unique_lock<std::mutex> lock);

  // Handle inbox being readable.
  //
  // This is triggered from the reactor loop when this connection's
  // peer has written an entry into our inbox. It is called once per
  // message. Because it's called from another thread, we must always
  // take care to acquire the connection's lock here.
  void handleInboxReadable();

  // Handle outbox being writable.
  //
  // This is triggered from the reactor loop when this connection's
  // peer has read an entry from our outbox. It is called once per
  // message. Because it's called from another thread, we must always
  // take care to acquire the connection's lock here.
  void handleOutboxWritable();

 private:
  std::mutex mutex_;
  State state_{INITIALIZING};
  Error error_;
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Reactor> reactor_;
  std::shared_ptr<Socket> socket_;

  // Inbox.
  int inboxHeaderFd_;
  int inboxDataFd_;
  optional<TConsumer> inbox_;
  optional<Reactor::TToken> inboxReactorToken_;

  // Outbox.
  optional<TProducer> outbox_;
  optional<Reactor::TToken> outboxReactorToken_;

  // Peer trigger/tokens.
  optional<Reactor::Trigger> peerReactorTrigger_;
  optional<Reactor::TToken> peerInboxReactorToken_;
  optional<Reactor::TToken> peerOutboxReactorToken_;

  // Reads happen only if the user supplied a callback (and optionally
  // a destination buffer). The callback is run from the event loop
  // thread upon receiving a notification from our peer.
  //
  // The memory pointer argument to the callback is valid only for the
  // duration of the callback. If the memory contents must be
  // preserved for longer, it must be copied elsewhere.
  //
  class ReadOperation {
   public:
    explicit ReadOperation(void* ptr, size_t len, read_callback_fn fn);

    explicit ReadOperation(read_callback_fn fn);

    // Processes a pending read.
    void handleRead(TConsumer& consumer);

    void handleError(const Error& error);

   private:
    void* ptr_{nullptr};
    size_t len_{0};
    read_callback_fn fn_;
  };

  // Writes happen only if the user supplied a memory pointer, the
  // number of bytes to write, and a callback to execute upon
  // completion of the write.
  //
  // The memory pointed to by the pointer may only be reused or freed
  // after the callback has been called.
  //
  class WriteOperation {
   public:
    using write_fn = std::function<bool(TProducer&)>;
    WriteOperation(const void* ptr, size_t len, write_callback_fn fn);
    WriteOperation(write_fn writer, write_callback_fn fn);

    bool handleWrite(TProducer& producer);

    void handleError(const Error& error);

   private:
    const void* ptr_{nullptr};
    size_t len_{0};
    write_fn writer_;
    write_callback_fn fn_;
  };

  class RingBufferZeroCopyOutputStream
      : public google::protobuf::io::ZeroCopyOutputStream {
   public:
    RingBufferZeroCopyOutputStream(TProducer* buffer, size_t payloadSize);

    bool Next(void** data, int* size) override;

    void BackUp(int /* unused */) override;

    int64_t ByteCount() const override;

   private:
    TProducer* buffer_;
    const size_t payloadSize_;
    int64_t bytesCount_{0};
  };

  // Pending read operations.
  std::deque<ReadOperation> readOperations_;
  size_t readOperationsPending_{0};

  // Pending write operations.
  std::deque<WriteOperation> writeOperations_;
  size_t writeOperationsPending_{0};

  // Defer execution of processReadOperations to loop thread.
  void triggerProcessReadOperations();

  // Process pending read operations if in an operational state.
  void processReadOperations(std::unique_lock<std::mutex> lock);

  // Defer execution of processWriteOperations to loop thread.
  void triggerProcessWriteOperations();

  // Process pending write operations if in an operational state.
  void processWriteOperations(std::unique_lock<std::mutex> lock);

  // Set error object while holding mutex.
  void setErrorHoldingMutex(Error&&);

  // Fail with error while holding mutex.
  void failHoldingMutex(Error&&);

  // Close connection.
  void close();

  // Close connection while holding mutex.
  void closeHoldingMutex();
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
