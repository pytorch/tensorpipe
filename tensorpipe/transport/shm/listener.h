#pragma once

#include <functional>
#include <memory>
#include <vector>

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
  using transport::Listener::connection_callback_fn;

  Listener(
      std::shared_ptr<Loop> loop,
      const Sockaddr& addr,
      connection_callback_fn fn);

  ~Listener() override;

  void handleEvents(int events) override;

 private:
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> listener_;
  connection_callback_fn fn_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
