/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <deque>
#include <memory>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Context;
class Listener;
class TCPHandle;

class Connection : public transport::Connection,
                   public std::enable_shared_from_this<Connection> {
  // Use the passkey idiom to allow make_shared to call what should be a private
  // constructor. See https://abseil.io/tips/134 for more information.
  struct ConstructorToken {};

 public:
  Connection(
      ConstructorToken,
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  ~Connection() override;

  using transport::Connection::read_callback_fn;

  void read(read_callback_fn fn) override;

  void read(void* ptr, size_t length, read_callback_fn fn) override;

  using transport::Connection::write_callback_fn;

  void write(const void* ptr, size_t length, write_callback_fn fn) override;

 private:
  // Create a connection that connects to the specified address.
  static std::shared_ptr<Connection> create(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  // Create a connection that is already connected (e.g. from a listener).
  static std::shared_ptr<Connection> create(
      std::shared_ptr<Loop> loop,
      std::shared_ptr<TCPHandle> handle);

  // Called to initialize member fields that need `shared_from_this`.
  void init();

  class Impl : public std::enable_shared_from_this<Impl> {
   public:
    Impl(std::shared_ptr<Loop>, std::shared_ptr<TCPHandle>);

    // Called to initialize member fields that need `shared_from_this`.
    void initFromLoop();

    void closeFromLoop();

    void closeCallbackFromLoop();

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
      void allocFromLoop(uv_buf_t* buf);

      // Called when libuv has read data from connection.
      void readFromLoop(ssize_t nread, const uv_buf_t* buf);

      // Returns if this read operation is complete.
      inline bool completeFromLoop() const {
        return mode_ == COMPLETE;
      }

      // Invoke user callback.
      inline void callbackFromLoop(const Error& error) {
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

    // Called from the event loop (deferred to it by the public methods).
    void readFromLoop(read_callback_fn fn);
    void readFromLoop(void* ptr, size_t length, read_callback_fn fn);

    // Called when libuv is about to read data from connection.
    void allocCallbackFromLoop(uv_buf_t* buf);

    // Called when libuv has read data from connection.
    void readCallbackFromLoop(ssize_t nread, const uv_buf_t* buf);

    // The write operation captures all state associated with writing a
    // fixed length chunk of data from the underlying connection. The
    // write includes a word-sized header containing the length of the
    // write. This header is a member field on this class and therefore
    // the instance must be kept alive and the reference to the instance
    // must remain valid until the write callback has been called.
    class WriteOperation {
     public:
      WriteOperation(const void* ptr, size_t length, write_callback_fn fn);

      inline std::tuple<uv_buf_t*, unsigned int> getBufs() {
        return std::make_tuple(bufs_.data(), bufs_.size());
      }

      // Invoke user callback.
      inline void callbackFromLoop(const Error& error) {
        fn_(error);
      }

     private:
      const char* ptr_;
      const size_t length_;

      // Buffers (structs with pointers and lengths) we pass to uv_write.
      std::array<uv_buf_t, 2> bufs_;

      // User callback.
      write_callback_fn fn_;
    };

    // Called from the event loop (deferred to it by the public methods).
    void writeFromLoop(const void* ptr, size_t length, write_callback_fn fn);

    // Called when libuv has written data to connection.
    void writeCallbackFromLoop(int status);

    std::deque<ReadOperation> readOperations_;
    std::deque<WriteOperation> writeOperations_;

    // By having the instance store a shared_ptr to itself we create a reference
    // cycle which will "leak" the instance. This allows us to detach its
    // lifetime from the connection and sync it with the TCPHandle's life cycle.
    std::shared_ptr<Impl> leak_;
  };

  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Impl> impl_;

  friend class Context;
  friend class Listener;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
