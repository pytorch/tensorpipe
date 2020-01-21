#pragma once

#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/shm/loop.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Context final : public transport::Context {
 public:
  explicit Context();

  ~Context() override;

  std::shared_ptr<transport::Connection> connect(address_t addr) override;

  std::shared_ptr<transport::Listener> listen(address_t addr) override;

  void join();

 private:
  std::shared_ptr<Loop> loop_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
