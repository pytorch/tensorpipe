#pragma once

#include <functional>
#include <memory>

#include <tensorpipe/transport/defs.h>

namespace tensorpipe {
namespace transport {

class Connection;

class Listener {
 public:
  virtual ~Listener() = default;

  using accept_callback_fn = std::function<void(std::shared_ptr<Connection>)>;

  virtual void accept(accept_callback_fn fn) = 0;

  // Return address that this listener is listening on.
  // This may be required if the listening address is not known up
  // front, or dynamically populated by the operating system (e.g. by
  // letting the operating system pick a TCP port to listen on).
  virtual address_t addr() const = 0;
};

} // namespace transport
} // namespace tensorpipe
