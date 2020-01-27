#pragma once

#include <memory>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/defs.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace transport {

class Context {
 public:
  virtual ~Context() = default;

  virtual void join() = 0;

  virtual std::shared_ptr<Connection> connect(address_t addr) = 0;

  virtual std::shared_ptr<Listener> listen(address_t addr) = 0;
};

} // namespace transport
} // namespace tensorpipe
