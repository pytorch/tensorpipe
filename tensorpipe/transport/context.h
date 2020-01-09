#pragma once

#include <memory>
#include <string>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace transport {

class Context {
 public:
  using address_t = std::string;

  virtual ~Context() = default;

  virtual std::shared_ptr<Connection> connect(address_t addr) = 0;

  virtual std::shared_ptr<Listener> listen(
      address_t addr,
      Listener::connection_callback_fn fn) = 0;
};

} // namespace transport
} // namespace tensorpipe
