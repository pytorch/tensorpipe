#pragma once

#include <deque>
#include <memory>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Listener;
class TCPHandle;

class Connection : public transport::Connection,
                   public std::enable_shared_from_this<Connection> {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  using transport::Connection::read_callback_fn;
  using transport::Connection::write_callback_fn;

  // Create a connection that connects to the specified address.
  static std::shared_ptr<Connection> create(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  // Create a connection that is already connected (e.g. from a listener).
  static std::shared_ptr<Connection> create(
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  Connection(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  ~Connection() override;

  void read(read_callback_fn fn) override;

  void read(void* ptr, size_t length, read_callback_fn fn) override;

  void write(const void* ptr, size_t length, write_callback_fn fn) override;

 protected:
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<TCPHandle> handle_;
  Error error_{Error::kSuccess};

  // The read operation captures all state associated with reading a
  // fixed length chunk of data from the underlying connection. All
  // reads are required to include a word-sized header containing the
  // number of bytes in the operation. This makes it possible for the
  // read side of the connection to either 1) not know how many bytes
  // to expected, and dynamically allocate, or 2) know how many bytes
  // to expect, and preallocate the destination memory.
  class ReadOperation {
    enum Mode {
      READ_LENGTH,
      READ_PAYLOAD,
      COMPLETE,
    };

   public:
    explicit ReadOperation(read_callback_fn fn) : fn_(std::move(fn)) {}

    ReadOperation(void* ptr, size_t length, read_callback_fn fn)
        : ptr_(static_cast<char*>(ptr)),
          givenLength_(length),
          fn_(std::move(fn)) {}

    // Called when libuv is about to read data from connection.
    void alloc(uv_buf_t* buf);

    // Called when libuv has read data from connection.
    void read(ssize_t nread, const uv_buf_t* buf);

    // Returns if this read operation is complete.
    inline bool complete() const {
      return mode_ == COMPLETE;
    }

    // Invoke user callback.
    inline void callback(const Error& error) {
      fn_(error, ptr_, readLength_);
    }

   private:
    Mode mode_{READ_LENGTH};
    char* ptr_{nullptr};

    // Number of bytes as specified by the user (if applicable).
    optional<size_t> givenLength_;

    // Number of bytes to expect as read from the connection.
    size_t readLength_{0};

    // Number of bytes read from the connection.
    // This is reset to 0 when we advance from READ_LENGTH to READ_PAYLOAD.
    size_t bytesRead_{0};

    // Holds temporary allocation if no length was specified.
    std::unique_ptr<char[]> buffer_{nullptr};

    // User callback.
    read_callback_fn fn_;
  };

  // Called when libuv is about to read data from connection.
  void allocCallback(uv_buf_t* buf);

  // Bound std::function for `allocCallback`.
  std::function<void(uv_buf_t* buf)> allocCallback_;

  // Called when libuv has read data from connection.
  void readCallback(ssize_t nread, const uv_buf_t* buf);

  // Bound std::function for `readCallback`.
  std::function<void(ssize_t nread, const uv_buf_t* buf)> readCallback_;

  // The write operation captures all state associated with writing a
  // fixed length chunk of data from the underlying connection. The
  // write includes a word-sized header containing the length of the
  // write. This header is a member field on this class and therefore
  // the instance must be kept alive and the reference to the instance
  // must remain valid until the write callback has been called.
  class WriteOperation {
   public:
    WriteOperation(const void* ptr, size_t length, write_callback_fn fn)
        : ptr(static_cast<const char*>(ptr)),
          length(length),
          fn_(std::move(fn)) {}

    const char* ptr;
    const size_t length;

    // Invoke user callback.
    inline void callback(const Error& error) {
      fn_(error);
    }

   private:
    // User callback.
    write_callback_fn fn_;
  };

  // Called when libuv has written data to connection.
  void writeCallback(int status);

  // Note: the containers below must never invalidate references.
  std::mutex operationsMutex_;
  std::deque<ReadOperation> readOperations_;
  std::deque<WriteOperation> writeOperations_;

  friend class Listener;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
