#pragma once

#include <deque>
#include <memory>
#include <mutex>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/shm/loop.h>
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
    SEND_EVENTFD,
    RECV_EVENTFD,
    SEND_SEGMENT_PREFIX,
    RECV_SEGMENT_PREFIX,
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
  // Note that this is triggered from the monitor of the eventfd,
  // so the instance lock must be acquired here.
  void handleInboxReadable();

 private:
  std::mutex mutex_;
  State state_{INITIALIZING};
  Error error_;
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> socket_;

  // Inbox.
  Fd inboxEventFd_;
  std::string inboxSegmentPrefix_;
  optional<TConsumer> inbox_;

  // Outbox.
  Fd outboxEventFd_;
  std::string outboxSegmentPrefix_;
  optional<TProducer> outbox_;

  // Monitors the eventfd of the inbox.
  std::shared_ptr<FunctionEventHandler> inboxMonitor_;

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
    explicit ReadOperation(read_callback_fn fn);

    // Processes a pending read.
    void handleRead(TConsumer& consumer);

    void handleError(const Error& error);

   private:
    std::unique_ptr<char*> ptr_{};
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
    WriteOperation(const void* ptr, size_t len, write_callback_fn fn);

    void handleWrite(TProducer& producer);

    void handleError(const Error& error);

   private:
    const void* ptr_{nullptr};
    size_t len_{0};
    write_callback_fn fn_;
  };

  // Pending read operations.
  std::deque<ReadOperation> readOperations_;
  size_t readOperationsPending_{0};

  // Pending write operations.
  std::deque<WriteOperation> writeOperations_;
  size_t writeOperationsPending_{0};

  // Read value from the inbox eventfd and increment
  // readOperationsPending_ if applicable. If the eventfd is not
  // readable, this is a no-op.
  void readInboxEventFd();

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
