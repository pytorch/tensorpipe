#pragma once

#include <functional>
#include <memory>
#include <vector>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/listener.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/socket.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Listener final : public transport::Listener,
                       public std::enable_shared_from_this<Listener>,
                       public EventHandler {
 public:
  static std::shared_ptr<Listener> create(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr);

  using transport::Listener::accept_callback_fn;

  Listener(std::shared_ptr<Loop> loop, const Sockaddr& addr);

  ~Listener() override;

  void accept(accept_callback_fn fn) override;

  void handleEvents(int events) override;

 private:
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> listener_;
  optional<accept_callback_fn> fn_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
