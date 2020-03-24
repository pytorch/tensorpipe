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

namespace tensorpipe {
namespace transport {
namespace shm {

class Connection final : public transport::Connection,
                         public std::enable_shared_from_this<Connection>,
                         public EventHandler {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  static constexpr auto kDefaultSize = 2 * 1024 * 1024;

  enum State {
    INITIALIZING = 1,
    INITIALIZING_ERROR,
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
  void read(google::protobuf::MessageLite& message, read_proto_callback_fn fn)
      override;

  // Implementation of transport::Connection.
  void read(void* ptr, size_t length, read_callback_fn fn) override;

  // Implementation of transport::Connection
  void write(const void* ptr, size_t length, write_callback_fn fn) override;

  // Implementation of transport::Connection
  void write(const google::protobuf::MessageLite& message, write_callback_fn fn)
      override;

  // Implementation of EventHandler.
  void handleEventsFromReactor(int events) override;

  // Handle events of type EPOLLIN.
  void handleEventInFromReactor(std::unique_lock<std::mutex> lock);

  // Handle events of type EPOLLOUT.
  void handleEventOutFromReactor(std::unique_lock<std::mutex> lock);

  // Handle events of type EPOLLERR.
  void handleEventErrFromReactor(std::unique_lock<std::mutex> lock);

  // Handle events of type EPOLLHUP.
  void handleEventHupFromReactor(std::unique_lock<std::mutex> lock);

  // Handle inbox being readable.
  //
  // This is triggered from the reactor loop when this connection's
  // peer has written an entry into our inbox. It is called once per
  // message. Because it's called from another thread, we must always
  // take care to acquire the connection's lock here.
  void handleInboxReadableFromReactor();

  // Handle outbox being writable.
  //
  // This is triggered from the reactor loop when this connection's
  // peer has read an entry from our outbox. It is called once per
  // message. Because it's called from another thread, we must always
  // take care to acquire the connection's lock here.
  void handleOutboxWritableFromReactor();

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
  optional<util::ringbuffer::Consumer> inbox_;
  optional<Reactor::TToken> inboxReactorToken_;

  // Outbox.
  optional<util::ringbuffer::Producer> outbox_;
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
    enum Mode {
      READ_LENGTH,
      READ_PAYLOAD,
    };

   public:
    using read_fn = std::function<ssize_t(util::ringbuffer::Consumer&)>;
    explicit ReadOperation(void* ptr, size_t len, read_callback_fn fn);
    explicit ReadOperation(read_fn reader, read_callback_fn fn);
    explicit ReadOperation(read_callback_fn fn);

    // Processes a pending read.
    bool handleRead(util::ringbuffer::Consumer& consumer);

    bool completed() const {
      return (mode_ == READ_PAYLOAD && bytesRead_ == len_);
    }

    void handleError(const Error& error);

   private:
    Mode mode_{READ_LENGTH};
    void* ptr_{nullptr};
    read_fn reader_;
    std::unique_ptr<uint8_t[]> buf_;
    size_t len_{0};
    size_t bytesRead_{0};
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
    enum Mode {
      WRITE_LENGTH,
      WRITE_PAYLOAD,
    };

   public:
    using write_fn = std::function<ssize_t(util::ringbuffer::Producer&)>;
    WriteOperation(const void* ptr, size_t len, write_callback_fn fn);
    WriteOperation(write_fn writer, write_callback_fn fn);

    bool handleWrite(util::ringbuffer::Producer& producer);

    bool completed() const {
      return (mode_ == WRITE_PAYLOAD && bytesWritten_ == len_);
    }

    void handleError(const Error& error);

   private:
    Mode mode_{WRITE_LENGTH};
    const void* ptr_{nullptr};
    write_fn writer_;
    size_t len_{0};
    size_t bytesWritten_{0};
    write_callback_fn fn_;
  };

  // Pending read operations.
  std::deque<ReadOperation> readOperations_;

  // Pending write operations.
  std::deque<WriteOperation> writeOperations_;

  // Defer execution of processReadOperations to loop thread.
  void triggerProcessReadOperations();

  // Process pending read operations if in an operational or error state.
  void processReadOperationsFromReactor(std::unique_lock<std::mutex>& lock);

  // Defer execution of processWriteOperations to loop thread.
  void triggerProcessWriteOperations();

  // Process pending write operations if in an operational state.
  void processWriteOperationsFromReactor(std::unique_lock<std::mutex>& lock);

  // Set error object while holding mutex.
  void setErrorHoldingMutexFromReactor(Error&&);

  // Fail with error while holding mutex.
  void failHoldingMutexFromReactor(Error&&, std::unique_lock<std::mutex>& lock);

  // Close connection.
  void close();

  // Close connection while holding mutex.
  void closeHoldingMutex();
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
