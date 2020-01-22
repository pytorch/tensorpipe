#pragma once

#include <memory>

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

  friend class Listener;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
