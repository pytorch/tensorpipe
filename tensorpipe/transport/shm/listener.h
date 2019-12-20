#pragma once

#include <memory>

#include <tensorpipe/transport/listener.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/socket.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Listener final : public transport::Listener,
                       public std::enable_shared_from_this<Listener>,
                       public EventHandler {
 public:
  Listener(std::shared_ptr<Loop> loop, const std::string& name);

  ~Listener() override;

  void handleEvents(int events) override;

 private:
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> listener_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
