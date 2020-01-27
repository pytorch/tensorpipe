#pragma once

#include <memory>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/listener.h>

#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class TCPHandle;

class Listener : public transport::Listener,
                 public std::enable_shared_from_this<Listener> {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  using transport::Listener::accept_callback_fn;

  static std::shared_ptr<Listener> create(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  Listener(ConstructorToken, std::shared_ptr<Loop> loop, const Sockaddr& addr);

  ~Listener() override;

 private:
  void start();

 public:
  Sockaddr sockaddr();

  void accept(accept_callback_fn fn) override;

  address_t addr() const override;

 protected:
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<TCPHandle> listener_;
  optional<accept_callback_fn> fn_;

  // This function is called by the event loop if the listening socket can
  // accept a new connection. Status is 0 in case of success, < 0
  // otherwise. See `uv_connection_cb` for more information.
  void connectionCallback(int status);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
