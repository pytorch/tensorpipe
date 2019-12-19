#pragma once

#include <memory>
#include <string>

namespace tensorpipe {
namespace transport {

class Connection;
class Listener;

class Context {
 public:
  using address_t = std::string;

  virtual ~Context() = default;

  virtual std::shared_ptr<Connection> connect(address_t addr) = 0;

  virtual std::shared_ptr<Listener> listen(address_t addr) = 0;
};

} // namespace transport
} // namespace tensorpipe
